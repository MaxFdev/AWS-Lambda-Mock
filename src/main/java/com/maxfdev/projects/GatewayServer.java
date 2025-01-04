package com.maxfdev.projects;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.maxfdev.projects.Message.MessageType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class GatewayServer extends Thread implements LoggingServer {

    private final Logger logger;
    private final HttpServer httpServer;
    private final ConcurrentHashMap<Integer, Message> responseCache;
    private final GatewayPeerServerImpl gatewayPeerServerImpl;
    private final GatewayRequestHandler gatewayRequestHandler;
    private InetSocketAddress currentLeader;

    public GatewayServer(
            int httpPort,
            int peerPort,
            long peerEpoch,
            Long serverID,
            ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress,
            int numberOfObservers)
            throws IOException {

        this.logger = initializeLogging("GatewayServer", true);
        this.logger.info("Constructing GatewayServer - HTTP:"
                + httpPort
                + " Peer UDP:"
                + peerPort
                + " TCP:"
                + (peerPort + 2));

        setName("GatewayServer");

        this.gatewayPeerServerImpl = new GatewayPeerServerImpl(
                peerPort,
                0,
                serverID,
                peerIDtoAddress,
                serverID,
                numberOfObservers);

        // set up http server
        int limit = Math.max(Runtime.getRuntime().availableProcessors(), 6);
        this.httpServer = HttpServer.create(new InetSocketAddress("localhost", httpPort), 0);
        this.httpServer.setExecutor(new ThreadPoolExecutor(
                4,
                limit,
                500,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()));
        this.responseCache = new ConcurrentHashMap<Integer, Message>();
        this.gatewayRequestHandler = new GatewayRequestHandler(
                this.gatewayPeerServerImpl,
                this.responseCache,
                new AtomicLong(0));
        this.httpServer.createContext("/compileandrun", this.gatewayRequestHandler);

        // set up endpoint for election status
        this.httpServer.createContext("/electionstatus", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Vote leader = gatewayPeerServerImpl.getCurrentLeader();
                if (gatewayPeerServerImpl.getCurrentLeader() == null) {
                    exchange.sendResponseHeaders(400, -1);
                } else {
                    logger.info("Election status relayed");
                    StringBuilder response = new StringBuilder();
                    for (long peerID : peerIDtoAddress.keySet()) {
                        response.append(peerID);
                        response.append(": ");
                        response.append(peerID == leader.getProposedLeaderID()
                                ? "leading\n"
                                : "following\n");
                    }
                    response.append(serverID);
                    response.append(": observing\n");
                    exchange.sendResponseHeaders(200, response.length());
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.toString().getBytes());
                }
            }
        });
    }

    @Override
    public void run() {
        this.logger.info("Starting to run");

        // start all threads
        this.httpServer.start();
        this.gatewayPeerServerImpl.start();

        this.logger.info("Http & Peer servers started");

        // process until stopped
        while (!isInterrupted()) {
            if (checkLeaderChange()) {
                // TODO make sure not to close listeners after leader is elected
                // (they may be talking to the right leader)
                this.gatewayRequestHandler.closeListeners();
            }
            onSpinWait();
        }

        this.logger.severe("Shutting down");
    }

    public GatewayPeerServerImpl getPeerServer() {
        return this.gatewayPeerServerImpl;
    }

    public void shutdown() {
        this.logger.info("Shutdown called");
        interrupt();
        this.httpServer.stop(0);
        this.gatewayRequestHandler.closeListeners();
        this.gatewayPeerServerImpl.shutdown();
    }

    /**
     * Get the current leader, and wait if none is elected.
     * 
     * @return the leader UDP address
     */
    private boolean checkLeaderChange() {
        InetSocketAddress leader = this.gatewayPeerServerImpl.getLeaderAddress();
        boolean change;
        if (leader == null) {
            change = this.currentLeader != null;
            this.currentLeader = leader;
            return change;
        } else {
            change = !leader.equals(this.currentLeader);
            this.currentLeader = leader;
            return change;
        }
    }

    /**
     * A class for handling all incoming http requests from clients.
     */
    private class GatewayRequestHandler implements HttpHandler, LoggingServer {

        // private final Logger logger;
        private final GatewayPeerServerImpl gatewayPeerServerImpl;
        private final ConcurrentHashMap<Integer, Message> responseCache;
        private final AtomicLong IDGenerator;
        private final ConcurrentSkipListSet<ServerSocket> activeListeners;

        private GatewayRequestHandler(
                GatewayPeerServerImpl gatewayPeerServerImpl,
                ConcurrentHashMap<Integer, Message> responseCache,
                AtomicLong IDGenerator)
                throws IOException {
            this.gatewayPeerServerImpl = gatewayPeerServerImpl;
            this.responseCache = responseCache;
            this.IDGenerator = IDGenerator;
            this.activeListeners = new ConcurrentSkipListSet<ServerSocket>(
                    Comparator.comparingInt(ServerSocket::getLocalPort));
        }

        public void closeListeners() {
            for (ServerSocket listener : this.activeListeners.toArray(
                    new ServerSocket[this.activeListeners.size()])) {
                try {
                    listener.close();
                } catch (IOException e) {
                }
                this.activeListeners.remove(listener);
            }
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // get the new request ID
            long requestID = this.IDGenerator.getAndIncrement();

            // set up logging
            Logger logger = initializeLogging("GatewayRequestHandler-" + requestID, true);
            logger.info("Starting handler for request: " + requestID);

            // get request details
            String requestMethod = exchange.getRequestMethod();
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            byte[] requestBytes = Util.readAllBytesFromNetwork(exchange.getRequestBody());
            String requestBody = new String(requestBytes);

            logger.info("Request received on "
                    + exchange.getLocalAddress().getPort()
                    + " from "
                    + exchange.getRemoteAddress().getPort()
                    + "\nType: "
                    + requestMethod
                    + "\nContent-Type: "
                    + (contentType == null ? "null" : contentType)
                    + "\nContent:\n"
                    + requestBody);

            // check that the request is valid
            if (!isValidRequest(exchange, requestMethod, contentType, logger)) {
                return;
            }

            // check the cache
            Message cachedMessage = this.responseCache.get(requestBody.hashCode());
            if (isCached(exchange, cachedMessage, logger)) {
                return;
            }

            // send work until there is a valid response
            ServerSocket listener = null;
            Message response = null;
            while (!isInterrupted() && response == null) {
                // set up a server socket as a return address
                while (!isInterrupted() && (listener == null || listener.isClosed())) {
                    try {
                        listener = new ServerSocket(0);
                    } catch (IOException e) {
                        listener = null;
                        logger.severe("IOE thrown while setting up return message socket server:\n"
                                + e.getLocalizedMessage()
                                + "\nRetrying");
                    }
                }
                this.activeListeners.add(listener);

                // get the leader address
                InetSocketAddress leaderUDPAddress;
                InetSocketAddress leaderTCPAddress;
                try {
                    leaderUDPAddress = getLeader();
                    leaderTCPAddress = new InetSocketAddress(leaderUDPAddress.getHostName(),
                            leaderUDPAddress.getPort() + 2);
                } catch (InterruptedException e) {
                    logger.severe("Interrupted while getting leader");
                    break;
                }

                // create work message
                Message work = new Message(
                        MessageType.WORK,
                        requestBytes,
                        listener.getInetAddress().getHostName(),
                        listener.getLocalPort(),
                        leaderTCPAddress.getHostName(),
                        leaderTCPAddress.getPort(),
                        requestID);

                // send work to leader
                try {
                    sendLeaderWork(work, leaderTCPAddress, logger);
                } catch (Exception e) {
                    logger.severe("Exception thrown while sending work to leader:\n"
                            + e.getLocalizedMessage()
                            + "\nRetrying (after 1 sec)");
                    // retry in a second
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        break;
                    }
                    continue;
                }

                // get response
                try {
                    response = getResponseMessage(listener, leaderUDPAddress, logger);
                } catch (Exception e) {
                    logger.severe("Exception thrown while listening for work return:\n"
                            + e.getLocalizedMessage()
                            + "\nRetrying");
                }
            }

            // check if the the handler was interrupted
            if (isInterrupted()) {
                if (listener != null) {
                    listener.close();
                    this.activeListeners.remove(listener);
                }
                logger.severe("Interrupted - shutting down");
                return;
            }

            // cache the response
            this.responseCache.put(requestBody.hashCode(), response);

            // respond with response
            try {
                int code = response.getErrorOccurred() ? 400 : 200;
                exchange.getResponseHeaders().add("Cached-Response", "false");
                exchange.sendResponseHeaders(code, response.getMessageContents().length);
                OutputStream respond = exchange.getResponseBody();
                respond.write(response.getMessageContents());
                respond.close();
            } catch (IOException e) {
                logger.severe("IOE thrown while sending response to client:\n"
                        + e.getLocalizedMessage());
                return;
            }

            logger.info("Response returned - exiting");
        }

        /**
         * Check whether a client request is valid or not. If not send appropriate
         * response.
         * 
         * @param exchange
         * @param requestMethod
         * @param contentType
         * @return true iff the request is valid
         */
        private boolean isValidRequest(HttpExchange exchange, String requestMethod, String contentType, Logger logger) {
            try {
                // check for post, else return 405
                if (requestMethod == null || !requestMethod.equalsIgnoreCase("POST")) {
                    String response = "Invalid request method for context";
                    exchange.getResponseHeaders().add("Cached-Response", "false");
                    exchange.sendResponseHeaders(405, response.length());
                    OutputStream respond = exchange.getResponseBody();
                    respond.write(response.getBytes());
                    respond.close();

                    logger.info("Bad request method - returning 405");

                    return false;
                }
                // check for content type "text/x-java-source", else 400
                else if (contentType == null || !contentType.equals("text/x-java-source")) {
                    String response = "Invalid Content-Type";
                    exchange.getResponseHeaders().add("Cached-Response", "false");
                    exchange.sendResponseHeaders(400, response.length());
                    OutputStream respond = exchange.getResponseBody();
                    respond.write(response.getBytes());
                    respond.close();

                    logger.info("Bad/missing Content-Type - returning 400");

                    return false;
                } else {
                    return true;
                }
            } catch (IOException e) {
                logger.severe("IOE thrown while validating request:\n"
                        + e.getLocalizedMessage());

                // return false in order to exit
                return false;
            }
        }

        /**
         * Check if the request is in cache. If so respond with the cached message.
         * 
         * @param exchange
         * @param cachedMessage
         * @return true iff the request is in cache
         */
        private boolean isCached(HttpExchange exchange, Message cachedMessage, Logger logger) {
            try {
                if (cachedMessage != null) {
                    String cacheResponse = new String(cachedMessage.getMessageContents());
                    exchange.getResponseHeaders().add("Cached-Response", "true");
                    exchange.sendResponseHeaders(cachedMessage.getErrorOccurred() ? 400 : 200, cacheResponse.length());
                    OutputStream respond = exchange.getResponseBody();
                    respond.write(cacheResponse.getBytes());
                    respond.close();

                    logger.fine("Returning cached response:\n" + cacheResponse);

                    return true;
                }
                return false;
            } catch (IOException e) {
                logger.severe("IOE thrown while dealing with cache:\n"
                        + e.getLocalizedMessage());

                // return true in order to exit
                return true;
            }
        }

        /**
         * Get the current leader, and wait if none is elected.
         * 
         * @return the leader UDP address
         */
        private InetSocketAddress getLeader() throws InterruptedException {
            InetSocketAddress leader;
            while ((leader = this.gatewayPeerServerImpl.getLeaderAddress()) == null && !isInterrupted()) {
                Thread.onSpinWait();
            }
            if (isInterrupted()) {
                throw new InterruptedException();
            }
            return leader;
        }

        /**
         * Send work to the leader at the specified address.
         * 
         * @param work
         * @param leaderTCPAddress
         * @throws IOException
         */
        private void sendLeaderWork(Message work, InetSocketAddress leaderTCPAddress, Logger logger)
                throws IOException {
            Socket workMessageSocket = new Socket();
            workMessageSocket.connect(leaderTCPAddress);
            workMessageSocket.getOutputStream().write(work.getNetworkPayload());
            workMessageSocket.getOutputStream().flush();
            workMessageSocket.close();

            logger.fine("Work sent from "
                    + workMessageSocket.getLocalPort()
                    + " to leader on "
                    + workMessageSocket.getPort());
        }

        /**
         * Get the response message from the leader. Check after receiving response that
         * the leader has not changed. If the leader has changed, return null.
         * 
         * @param listener
         * @param leaderUDPAddress
         * @return completed work message or null if response is invalid
         * @throws IOException
         */
        private Message getResponseMessage(ServerSocket listener, InetSocketAddress leaderUDPAddress, Logger logger)
                throws IOException {
            logger.fine("Listening for response on " + listener.getLocalPort());

            // set up return string and code
            Message response = new Message(
                    Util.readAllBytesFromNetwork(listener.accept().getInputStream()));
            listener.close();
            this.activeListeners.remove(listener);

            logger.fine("Response received on "
                    + listener.getLocalPort()
                    + ":\n"
                    + new String(response.getMessageContents()));

            if (!this.gatewayPeerServerImpl.isPeerDead(leaderUDPAddress)
                    && this.gatewayPeerServerImpl.getLeaderAddress().equals(leaderUDPAddress)) {
                return response;
            } else {
                logger.info("Leader failed - resending work to new leader");
                return null;
            }
        }
    }
}
