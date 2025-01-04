package com.maxfdev.projects;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.maxfdev.projects.Message.MessageType;

// TODO test worker failure
public class RoundRobinLeader extends Thread implements LoggingServer {

    private final Logger logger;
    private final PeerServer server;
    private final int tcpPort;
    private final List<InetSocketAddress> workers;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final MessageInputServer messageInputServer;
    private final Map<Long, Message> cachedWork;
    private final AtomicInteger cacheResponders;
    private final ExecutorService threadPool;
    private long requestCount;

    public RoundRobinLeader(
            PeerServer server,
            int tcpPort,
            List<InetSocketAddress> workers,
            Message retiredWorkerCache)
            throws IOException {
        this.logger = initializeLogging("RoundRobinLeader-" + server.getServerId(), true);
        this.logger.info("Constructing RoundRobinLeader on TCP:" + tcpPort);

        setDaemon(true);
        setName("RoundRobinLeader-" + server.getServerId());

        this.server = server;
        this.tcpPort = tcpPort;
        this.workers = workers;
        this.incomingMessages = new LinkedBlockingQueue<Message>();
        this.messageInputServer = new MessageInputServer(tcpPort, this.incomingMessages, server);
        this.cachedWork = new HashMap<Long, Message>();
        if (retiredWorkerCache != null) {
            this.logger.info("Received cache from retired worker thread:\n"
                    + new String(retiredWorkerCache.getMessageContents()));
            this.cachedWork.put(retiredWorkerCache.getRequestID(), retiredWorkerCache);
        }
        this.cacheResponders = new AtomicInteger(0);
        this.requestCount = 0;
        int limit = Math.max(Runtime.getRuntime().availableProcessors(), 4);
        this.threadPool = new ThreadPoolExecutor(
                2,
                limit,
                500,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setDaemon(true);
                        return thread;
                    }
                });
    }

    @Override
    public void run() {
        this.logger.info("RoundRobinLeader starting to run");

        // start the tcp server to bring in messages
        this.messageInputServer.start();

        // send out request for cached work and await responses
        retrieveCachedWork();
        while (!isInterrupted() && this.cacheResponders.get() != this.workers.size()) {
            Thread.onSpinWait();
        }

        while (!isInterrupted()) {
            // get the next message
            Message message;
            try {
                message = this.incomingMessages.take();
            } catch (InterruptedException e) {
                this.logger.severe("RoundRobinLeader Interrupted while waiting on client request");
                this.threadPool.shutdownNow();
                this.messageInputServer.shutdown();
                return;
            }

            if (!message.getMessageType().equals(MessageType.WORK)) {
                this.logger.info("Non-work message skipped");
                continue;
            }

            this.logger.fine("Work received:\n" + new String(message.getMessageContents()));

            // store the gateways return address
            InetSocketAddress returnTCPAddress = new InetSocketAddress(
                    message.getSenderHost(),
                    message.getSenderPort());

            // check for cached work
            Message cachedResponse = this.cachedWork.remove(message.getRequestID());
            if (cachedResponse != null) {
                this.logger.info("Request " + message.getRequestID() + " cached response enqueued to be sent");
                sendCachedResponse(cachedResponse, returnTCPAddress);
                continue;
            }

            // get the next worker in the cycle
            InetSocketAddress workerTCPAddress = null;
            while (workerTCPAddress == null) {
                synchronized (this.workers) {
                    int workerIndex = (int) (this.requestCount++ % this.workers.size());
                    workerTCPAddress = this.workers.get(workerIndex);
                }
                if (isWorkerDown(workerTCPAddress)) {
                    workerTCPAddress = null;
                }
            }

            this.logger.fine("Request returns to TCP:"
                    + returnTCPAddress.getPort()
                    + " and work routed to TCP:"
                    + workerTCPAddress.getPort());

            // send work to the worker
            try {
                this.threadPool.submit(
                        new WorkerRelay(
                                this.requestCount,
                                this,
                                this.incomingMessages,
                                message,
                                new Message(
                                        MessageType.WORK,
                                        message.getMessageContents(),
                                        this.server.getAddress().getHostName(),
                                        this.tcpPort,
                                        workerTCPAddress.getHostName(),
                                        workerTCPAddress.getPort(),
                                        message.getRequestID()),
                                returnTCPAddress,
                                workerTCPAddress));
            } catch (Exception e) {
                this.logger.severe("Exception thrown while submitting relay for request "
                        + message.getRequestID()
                        + ":\n"
                        + e.getLocalizedMessage());

                this.threadPool.shutdownNow();
                this.messageInputServer.shutdown();
                return;
            }

            this.logger.fine("Message request " + message.getRequestID() + " enqueued for relay");
        }

        // shutdown other threads
        this.threadPool.shutdownNow();
        this.messageInputServer.shutdown();

        this.logger.info("RoundRobing shutting down");

    }

    public void shutdown() {
        this.logger.info("RoundRobinLeader shutdown called");
        interrupt();
        this.threadPool.shutdownNow();
        this.messageInputServer.shutdown();
    }

    /**
     * Send out a request to each worker for cached work.
     */
    private void retrieveCachedWork() {
        this.logger.info("Sending out request for cached work");

        for (InetSocketAddress workerTCPAddress : this.workers) {
            this.threadPool.submit(() -> {
                // create request
                Message cacheRequest = new Message(
                        MessageType.NEW_LEADER_GETTING_LAST_WORK,
                        new byte[0],
                        this.server.getAddress().getHostName(),
                        this.tcpPort,
                        workerTCPAddress.getHostName(),
                        workerTCPAddress.getPort());

                this.logger.fine("Requesting cache from TCP:" + workerTCPAddress.getPort());

                // get the cached work
                Message completedWork = null;
                while (completedWork == null) {
                    // check for interruption
                    if (isInterrupted()) {
                        this.logger.severe("Interrupted while getting cache from " + workerTCPAddress.getPort());
                        return;
                    }

                    // try to get the cache
                    try (Socket tcpSocket = new Socket()) {
                        tcpSocket.connect(workerTCPAddress);
                        tcpSocket.getOutputStream().write(cacheRequest.getNetworkPayload());
                        tcpSocket.getOutputStream().flush();
                        long timer = System.currentTimeMillis();
                        while (tcpSocket.getInputStream().available() < 1) {
                            // ping the worker to make sure it is still there
                            if (System.currentTimeMillis() - timer > 1000) {
                                this.logger.fine("Pinging worker");
                                tcpSocket.getOutputStream().write(1);
                                tcpSocket.getOutputStream().flush();
                                timer = System.currentTimeMillis();
                            }
                            Thread.onSpinWait();
                        }
                        completedWork = new Message(Util.readAllBytesFromNetwork(tcpSocket.getInputStream()));
                    } catch (Exception e) {
                        this.logger.severe("Error retrieving cached work from worker on TCP:"
                                + workerTCPAddress.getPort()
                                + "\n"
                                + e.getLocalizedMessage()
                                + "\nRetrying");
                        completedWork = null;
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ie) {
                        }
                    }
                }

                this.logger.fine("TCP:" + workerTCPAddress.getPort() + " responded to cache request");

                // update the results with the leader
                if (completedWork.getRequestID() >= 0) {
                    this.cachedWork.put(completedWork.getRequestID(), completedWork);
                    this.logger.fine("Cached work received for request: "
                            + completedWork.getRequestID()
                            + " from TCP:"
                            + workerTCPAddress.getPort()
                            + "\n"
                            + new String(completedWork.getMessageContents()));
                }
                this.cacheResponders.incrementAndGet();
            });
        }
    }

    /**
     * Send back a cached response to the given gateway return address.
     * 
     * @param cachedResponse
     * @param returnTCPAddress
     */
    private void sendCachedResponse(Message cachedResponse, InetSocketAddress returnTCPAddress) {
        this.threadPool.submit(() -> {
            // create the return message
            Message clientReturnMessage = new Message(
                    cachedResponse.getMessageType(),
                    cachedResponse.getMessageContents(),
                    this.server.getAddress().getHostName(),
                    this.server.getAddress().getPort(),
                    returnTCPAddress.getHostName(),
                    returnTCPAddress.getPort(),
                    cachedResponse.getRequestID(),
                    cachedResponse.getErrorOccurred());

            // send the response
            while (!isInterrupted()) {
                try (Socket tcpSocket = new Socket()) {
                    tcpSocket.connect(returnTCPAddress);
                    tcpSocket.getOutputStream().write(clientReturnMessage.getNetworkPayload());
                    tcpSocket.getOutputStream().flush();
                    this.logger.info("Returned cached results to TCP:" + returnTCPAddress.getPort());
                    return;
                } catch (IOException e) {
                    this.logger.severe("Error returning cached work to TCP:"
                            + returnTCPAddress.getPort()
                            + "\n"
                            + e.getLocalizedMessage()
                            + "\nRetrying");
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        });
    }

    /**
     * Check if the given worker (TCP address) is down. If so remove it from the
     * worker pool.
     * 
     * @param workerTCPAddress
     * @return true iff worker is not dead
     */
    private boolean isWorkerDown(InetSocketAddress workerTCPAddress) {
        InetSocketAddress workerUDPAddress = new InetSocketAddress(
                workerTCPAddress.getHostName(),
                workerTCPAddress.getPort() - 2);
        boolean workerDown = this.server.isPeerDead(workerUDPAddress);
        if (workerDown) {
            synchronized (this.workers) {
                this.workers.remove(workerTCPAddress);
            }
            this.logger.info("Worker on port "
                    + workerTCPAddress.getPort()
                    + " failed - adjusting workflow accordingly");
        }
        return workerDown;
    }

    /**
     * A class for taking in tcp connections and passing messages along to the
     * leader.
     */
    private class MessageInputServer extends Thread implements LoggingServer {

        private final Logger logger;
        private final int tcpPort;
        private final LinkedBlockingQueue<Message> incomingMessages;
        private final ServerSocket tcpServerSocket;

        private MessageInputServer(int port, LinkedBlockingQueue<Message> incomingMessages, PeerServer server)
                throws IOException {
            this.logger = initializeLogging("MessageInputServer-" + server.getServerId(), true);
            this.logger.info("Constructing MessageInputServer on TCP:" + port);
            setDaemon(true);
            setName("MessageInputServer-" + server.getServerId());
            this.tcpPort = port;
            this.incomingMessages = incomingMessages;
            this.tcpServerSocket = new ServerSocket();
            this.tcpServerSocket.bind(new InetSocketAddress(server.getAddress().getHostName(), this.tcpPort));
        }

        @Override
        public void run() {
            this.logger.info("Starting MessageInputServer");

            // process incoming connections
            while (!isInterrupted()) {
                // keep track of message
                Message message;
                try {
                    this.logger.fine("Listening for message");

                    // take in new message
                    Socket connection = this.tcpServerSocket.accept();
                    message = new Message(Util.readAllBytesFromNetwork(connection.getInputStream()));

                    this.logger.fine("Received new message from handler on TCP:"
                            + connection.getPort()
                            + ":\n"
                            + new String(message.getMessageContents()));
                } catch (IOException e) {
                    this.logger.severe("IOE thrown while trying to accept a message:\n"
                            + e.getLocalizedMessage());
                    break;
                }

                // put the message in queue
                try {
                    this.incomingMessages.put(message);
                } catch (InterruptedException e) {
                    this.logger.severe("MessageInputServer interrupted while adding message to queue");
                    break;
                }
            }

            // close listener and shutdown
            try {
                this.tcpServerSocket.close();
            } catch (IOException e) {
                this.logger.severe("IOE thrown while closing ServerSocket:\n"
                        + e.getLocalizedMessage());
            }

            this.logger.info("Shutting down");
        }

        private void shutdown() {
            this.logger.info("Shutdown initiated");
            interrupt();
            try {
                this.tcpServerSocket.close();
            } catch (IOException e) {
                this.logger.severe("IOE thrown while closing ServerSocket:\n"
                        + e.getLocalizedMessage());
            }
        }
    }

    /**
     * A class for relaying a work message from leader to worker and retrieving the
     * completed response. After which, relaying completed work to the gateway.
     */
    private class WorkerRelay implements Runnable, LoggingServer {

        private final Logger logger;
        private final Message request;
        private final Message work;
        private final InetSocketAddress returnTCPAddress;
        private final InetSocketAddress workerTCPAddress;
        private final LinkedBlockingQueue<Message> incomingMessages;
        private final RoundRobinLeader leader;

        private WorkerRelay(
                long relayID,
                RoundRobinLeader leader,
                LinkedBlockingQueue<Message> incomingMessages,
                Message request,
                Message work,
                InetSocketAddress returnTCPAddress,
                InetSocketAddress workerTCPAddress)
                throws IOException {
            this.logger = initializeLogging("WorkerRelay-" + relayID, true);
            this.logger.info("WorkerRelay " + relayID + " constructing for request " + work.getRequestID());
            this.request = request;
            this.work = work;
            this.returnTCPAddress = returnTCPAddress;
            this.workerTCPAddress = workerTCPAddress;
            this.incomingMessages = incomingMessages;
            this.leader = leader;
        }

        @Override
        public void run() {
            this.logger.info("Starting WorkerRelay");

            // check if worker is alive
            if (this.leader.isWorkerDown(this.workerTCPAddress)) {
                // put request back on the work queue
                this.incomingMessages.add(request);
                this.logger.severe("Worker is down - requeuing work request");
                return;
            }

            // try to connect 3 times
            Socket tcpSocket = new Socket();
            Message workReturn = null;
            int tries = 0;
            while (!isInterrupted() && workReturn == null && tries++ < 3) {
                try {
                    // send work to worker
                    tcpSocket.connect(this.workerTCPAddress);
                    tcpSocket.getOutputStream().write(this.work.getNetworkPayload());
                    tcpSocket.getOutputStream().flush();

                    this.logger.fine("Relayed work from TCP:"
                            + tcpSocket.getLocalPort()
                            + " to worker TCP:"
                            + this.workerTCPAddress.getPort()
                            + "\nContents:\n"
                            + new String(this.work.getMessageContents()));

                    // wait for work to be returned
                    long timer = System.currentTimeMillis();
                    while (!isInterrupted() && tcpSocket.getInputStream().available() < 1) {
                        // ping the worker to make sure it is still there
                        if (System.currentTimeMillis() - timer > 1000) {
                            this.logger.fine("Pinging worker");
                            tcpSocket.getOutputStream().write(1);
                            tcpSocket.getOutputStream().flush();
                            timer = System.currentTimeMillis();
                        }
                        Thread.onSpinWait();
                    }
                    if (isInterrupted()) {
                        this.logger.severe("Interrupted");
                        return;
                    }
                    byte[] completedWork = Util.readAllBytesFromNetwork(tcpSocket.getInputStream());
                    tcpSocket.close();
                    workReturn = new Message(completedWork);

                    this.logger.fine("Received completed work back:\n" + new String(workReturn.getMessageContents()));
                } catch (Exception e) {
                    this.logger.severe("Exception thrown while sending work:\n" + e.getMessage() + "\nRetrying...");
                }
            }

            // check work was returned or if worker is down
            if (workReturn == null || this.leader.isWorkerDown(this.workerTCPAddress)) {
                // put request back on the work queue
                this.incomingMessages.add(request);
                try {
                    tcpSocket.close();
                } catch (IOException e) {
                }
                this.logger.severe("Failed to reach worker - requeuing work request");
                return;
            }

            if (isInterrupted()) {
                this.logger.severe("Interrupted");
                return;
            }

            // return completed work to gateway
            try {
                Message clientReturnMessage = new Message(
                        workReturn.getMessageType(),
                        workReturn.getMessageContents(),
                        tcpSocket.getLocalAddress().getHostAddress(),
                        tcpSocket.getLocalPort(),
                        this.returnTCPAddress.getHostName(),
                        this.returnTCPAddress.getPort(),
                        workReturn.getRequestID(),
                        workReturn.getErrorOccurred());
                tcpSocket = new Socket();
                tcpSocket.connect(this.returnTCPAddress);
                tcpSocket.getOutputStream().write(clientReturnMessage.getNetworkPayload());
                tcpSocket.getOutputStream().flush();
                tcpSocket.close();

                this.logger.fine("Relayed results from TCP:"
                        + tcpSocket.getLocalPort()
                        + " to gateway TCP:"
                        + tcpSocket.getPort());
            } catch (Exception e) {
                this.logger.severe("Exception thrown while returning completed work:\n" + e.getLocalizedMessage());
                return;
            }

            this.logger.info("Relay complete - shutting down");
        }
    }
}
