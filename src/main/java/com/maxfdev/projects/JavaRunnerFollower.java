package com.maxfdev.projects;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

import com.maxfdev.projects.Message.MessageType;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final Logger logger;
    private final PeerServer server;
    private final int tcpPort;
    private final ServerSocket serverSocket;
    private volatile Socket socket;
    private final JavaRunner javaRunner;
    private volatile Message completedCache;
    private volatile boolean busy;
    private String response;
    private boolean errorOccurred;

    public JavaRunnerFollower(PeerServer server, int tcpPort, Message cache) throws IOException {
        this.logger = initializeLogging("JavaRunnerFollower-" + server.getServerId(), true);
        this.logger.info("Constructing JavaRunnerFollower on TCP:" + tcpPort);

        setDaemon(true);
        setName("JavaRunnerFollower-" + server.getServerId());

        this.server = server;
        this.tcpPort = tcpPort;
        this.serverSocket = new ServerSocket(this.tcpPort);
        this.javaRunner = new JavaRunner();
        this.completedCache = cache;
        if (cache != null) {
            this.logger.info("Starting with cached work:\n" + new String(cache.getMessageContents()));
        }
        this.busy = false;
    }

    @Override
    public void run() {
        this.logger.info("JavaRunnerFollower starting to run");

        while (!isInterrupted()) {
            // get the next message
            Message request;
            try {
                this.logger.fine("Listening for message on TCP:" + this.tcpPort);

                this.socket = this.serverSocket.accept();
                this.busy = true;
                request = new Message(Util.readAllBytesFromNetwork(this.socket.getInputStream()));

                this.logger.fine("Incoming connection on "
                        + this.socket.getLocalPort()
                        + " from "
                        + this.socket.getPort()
                        + "\nWork ID: "
                        + request.getRequestID()
                        + "\nOrigin Port: "
                        + request.getSenderPort()
                        + "\nType: "
                        + request.getMessageType()
                        + "\nContents:\n"
                        + new String(request.getMessageContents()));
            } catch (Exception e) {
                this.logger.severe("Exception thrown while retrieving message:\n"
                        + e.getLocalizedMessage());
                continue;
            }

            // ignore messages if leader is dead
            Vote leader = this.server.getCurrentLeader();
            if (leader == null) {
                this.logger.fine("Leader failed - quitting");
                this.busy = false;
                break;
            }

            // ignore messages that are not from current leader
            long leaderID = leader.getProposedLeaderID();
            int leaderTCP = this.server.getPeerByID(leaderID).getPort() + 2;
            if (request.getSenderPort() != leaderTCP) {
                this.logger.fine("Ignoring invalid message - not from leader");
                this.busy = false;
                continue;
            }

            switch (request.getMessageType()) {
                // check if the leader is requesting cached results
                case MessageType.NEW_LEADER_GETTING_LAST_WORK -> {
                    // check if there is cached work
                    if (this.completedCache == null) {
                        // if not create empty response
                        this.completedCache = new Message(
                                MessageType.COMPLETED_WORK,
                                new byte[0],
                                request.getReceiverHost(),
                                request.getReceiverPort(),
                                request.getSenderHost(),
                                request.getSenderPort(),
                                -1);
                    } else {
                        // set the correct addresses
                        this.completedCache = new Message(
                                MessageType.COMPLETED_WORK,
                                this.completedCache.getMessageContents(),
                                request.getReceiverHost(),
                                request.getReceiverPort(),
                                request.getSenderHost(),
                                request.getSenderPort(),
                                this.completedCache.getRequestID(),
                                this.completedCache.getErrorOccurred());
                    }

                    // HACK this fails first time
                    // send the response
                    Exception err = null;
                    while (!isInterrupted() && err == null) {
                        try {
                            this.socket.getOutputStream().write(this.completedCache.getNetworkPayload());
                            this.socket.getOutputStream().flush();
                            err = null;
                        } catch (IOException e) {
                            err = e;
                            this.logger.severe("IOE thrown while returning cached message:\n"
                                    + e.getLocalizedMessage()
                                    + "\nRetrying");
                            Thread.onSpinWait();
                        }
                    }

                    this.logger.fine("Cached results returned for work ID (-1 = nothing cached): "
                            + this.completedCache.getRequestID());

                    // clear the cache and set the busy state so it may be read
                    this.completedCache = null;
                    this.busy = false;
                }

                // check if this is a normal request
                case MessageType.WORK -> {
                    // reset error tracker
                    this.errorOccurred = false;

                    // get the code and run it
                    InputStream code = new ByteArrayInputStream(request.getMessageContents());
                    compileAndRun(code);

                    // check if interrupted while working
                    if (isInterrupted()) {
                        this.logger.severe("Work interrupted - not completed");
                        closeSocket();
                        return;
                    }

                    this.logger.fine("Work results completed:\n" + this.response);

                    // assemble return message
                    Message results = new Message(
                            MessageType.COMPLETED_WORK,
                            this.response.getBytes(),
                            request.getReceiverHost(),
                            request.getReceiverPort(),
                            request.getSenderHost(),
                            request.getSenderPort(),
                            request.getRequestID(),
                            this.errorOccurred);

                    // cache the results incase of failure
                    this.completedCache = results;
                    this.busy = false;

                    this.logger.fine("Results cached incase of failure");

                    // return the results to leader (unless leader failed)
                    while (!isInterrupted() && !this.server.isPeerDead(leaderID)) {
                        try {
                            this.socket.getOutputStream().write(results.getNetworkPayload());
                            this.socket.getOutputStream().flush();
                            this.logger.fine("Results returned for work ID: " + request.getRequestID());
                            break;
                        } catch (IOException e) {
                            this.logger.severe("IOE thrown while returning completed work:\n"
                                    + e.getLocalizedMessage()
                                    + "\nRetrying");
                        }
                    }
                }

                // ignore invalid message types
                default -> {
                    this.logger.fine("Ignoring invalid message - invalid message type");
                }
            }
        }

        closeSocket();
        this.busy = false;
        this.logger.info("JavaRunnerFollower shutting down");
    }

    /**
     * Wait for the follower to be ready to return the cache, then return it.
     * 
     * @return what ever is in the cache
     */
    public Message awaitAndGrabCache() {
        while (this.busy) {
            Thread.onSpinWait();
        }
        return this.completedCache;
    }

    public boolean isWorking() {
        return this.busy;
    }

    public void shutdown() {
        this.logger.info("Shutdown called");
        interrupt();
        closeSocket();
    }

    private void closeSocket() {
        try {
            this.serverSocket.close();
            if (this.socket != null) {
                this.socket.close();
            }
        } catch (IOException e) {
            this.logger.severe("IOE thrown while closing sockets:\n"
                    + e.getLocalizedMessage());
        }

        // allow access to the cache
        this.busy = false;
    }

    /**
     * Take in an input stream with the clients code and run it via the
     * {@link JavaRunner}. Before returning the response message
     * are recorded.
     * 
     * @param inputStream = code to run
     */
    private void compileAndRun(InputStream inputStream) {
        try {
            this.response = this.javaRunner.compileAndRun(inputStream);
            this.logger.fine("Work completed without error");
        } catch (IllegalArgumentException | IOException | ReflectiveOperationException e) {
            this.logger.fine("Error encountered while trying to complete work");
            this.errorOccurred = true;

            // make a string builder to make the error response
            StringBuilder responseBuilder = new StringBuilder(512);

            // build an error response
            responseBuilder.append(e.getMessage());
            responseBuilder.append("\n");

            // retrieve the stack trace to be returned
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(bytes);
            e.printStackTrace(printStream);
            responseBuilder.append(bytes.toString());

            // set the response to the built response
            this.response = responseBuilder.toString();
        }

        // check for null output
        if (this.response == null) {
            this.response = "null";
        }
    }
}
