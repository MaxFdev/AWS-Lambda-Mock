package com.maxfdev.projects;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.maxfdev.projects.Message.MessageType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {

    private final InetSocketAddress myAddress;
    private final Long serverID;
    private final Long gatewayID;
    private final String serverType;
    private final int udpPort;
    private final int httpPort;
    private final int tcpPort;
    private final AtomicLong peerEpoch;
    private final Logger logger;
    private final Logger summary;
    private final Logger verbose;
    private final HttpServer logHost;
    private final int numberOfObservers;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    private volatile Vote currentLeader;
    private volatile ServerState state;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final UDPMessageSender UDPMessageSender;
    private final UDPMessageReceiver UDPMessageReceiver;
    private final Logger gossipLogger;
    private Gossiper gossiper;
    private JavaRunnerFollower followerThread;
    private RoundRobinLeader leaderThread;
    private volatile Message cachedWork;

    public PeerServerImpl(
            int udpPort,
            long peerEpoch,
            Long serverID,
            Map<Long, InetSocketAddress> peerIDtoAddress,
            Long gatewayID,
            int numberOfObservers)
            throws IOException {

        this.serverType = serverID == gatewayID ? "GatewayPeerServerImpl" : "PeerServerImpl";
        this.state = serverID == gatewayID ? ServerState.OBSERVER : ServerState.LOOKING;

        this.logger = initializeLogging(serverType + "-" + serverID, true);
        this.logger.info("Logging set up for "
                + serverType
                + "-"
                + serverID
                + " on port UDP:"
                + udpPort
                + " TCP:"
                + (udpPort + 2));

        setName(serverType + "-" + serverID);

        // set vars
        this.myAddress = new InetSocketAddress("localhost", udpPort);
        this.udpPort = udpPort;
        this.httpPort = udpPort + 1;
        this.tcpPort = udpPort + 2;
        this.serverID = serverID;
        this.gatewayID = gatewayID;
        this.peerEpoch = new AtomicLong(peerEpoch);
        this.numberOfObservers = numberOfObservers;
        this.peerIDtoAddress = peerIDtoAddress;

        // set states
        this.currentLeader = null;

        // set up udp messaging for leader election
        this.outgoingMessages = new LinkedBlockingQueue<Message>();
        this.incomingMessages = new LinkedBlockingQueue<Message>();
        this.UDPMessageSender = new UDPMessageSender(this.outgoingMessages, this.udpPort);
        this.UDPMessageReceiver = new UDPMessageReceiver(
                this.incomingMessages,
                this.myAddress,
                this.udpPort,
                this);

        // set up additional logging
        this.summary = initializeLogging(this.serverType + "-" + serverID + "-summary", true);
        this.verbose = initializeLogging(this.serverType + "-" + serverID + "-verbose", true);
        this.logHost = HttpServer.create(new InetSocketAddress("localhost", this.httpPort), 0);
        this.logHost.setExecutor(new ThreadPoolExecutor(
                0,
                1,
                50,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()));
        this.logHost.createContext("/summary", createLogHandler("summary", serverID));
        this.logHost.createContext("/verbose", createLogHandler("verbose", serverID));

        // set up gossip logger (for consistent logging)
        this.gossipLogger = initializeLogging("Gossiper-" + getServerId(), true);
    }

    @Override
    public void shutdown() {
        this.logger.info("Shutdown called - starting shutdown");
        interrupt();
        this.UDPMessageSender.shutdown();
        this.UDPMessageReceiver.shutdown();
        this.logHost.stop(0);
        if (this.gossiper != null) {
            this.gossiper.shutdown();
        }
        if (this.followerThread != null) {
            this.followerThread.shutdown();
        }
        if (this.leaderThread != null) {
            this.leaderThread.shutdown();
        }
    }

    @Override
    public void run() {
        this.logger.info("Started running");

        // step 1: create and run thread that sends broadcast messages
        this.UDPMessageSender.start();

        // step 2: create and run thread that listens for messages sent to this server
        this.UDPMessageReceiver.start();

        // start other threads
        this.logHost.start();
        // this.gossiper.start();

        this.logger.fine("Server gossiper, message sender, and message receiver running");

        // step 3: main server loop
        try {
            while (!isInterrupted()) {
                switch (getPeerState()) {
                    case LOOKING -> {
                        this.logger.fine("Server in looking state - starting leader election");

                        // clear the follower thread
                        this.followerThread = null;

                        // start leader election, set leader to the election winner
                        LeaderElection election = new LeaderElection(this, this.incomingMessages, this.logger);
                        setCurrentLeader(election.lookForLeader());

                        this.logger.fine("Election finished - leader elected: "
                                + this.currentLeader.getProposedLeaderID());

                        // set up gossip
                        this.gossiper = new Gossiper(
                                this,
                                new HashMap<>(this.peerIDtoAddress),
                                this.incomingMessages,
                                this.gossipLogger,
                                this.summary,
                                this.verbose);
                        this.gossiper.start();

                        String switchMessage = getServerId()
                                + ": switching from "
                                + ServerState.LOOKING
                                + " to "
                                + getPeerState();
                        this.summary.info(switchMessage);
                        System.out.println(switchMessage);
                    }
                    case FOLLOWING -> {
                        // make sure there is no leader thread
                        if (this.leaderThread != null) {
                            this.logger.fine("Stopping leader thread");
                            this.leaderThread.shutdown();
                            this.leaderThread = null;
                        }

                        // run a follower thread
                        if (this.followerThread == null) {
                            this.logger.fine("Starting follower thread");
                            this.followerThread = new JavaRunnerFollower(
                                    this,
                                    this.tcpPort,
                                    this.cachedWork);
                            this.followerThread.start();
                            this.cachedWork = null;
                        }
                    }
                    case LEADING -> {
                        // make sure there is no leader thread
                        if (this.followerThread != null) {
                            this.logger.fine("Stopping follower thread");
                            this.followerThread.shutdown();
                            this.followerThread = null;
                        }

                        // run a leader thread
                        if (this.leaderThread == null) {
                            this.logger.fine("Starting leader thread");
                            List<InetSocketAddress> workerTCPPorts = this.peerIDtoAddress.values()
                                    .stream()
                                    .filter(addr -> !addr.equals(this.peerIDtoAddress.get(this.gatewayID)))
                                    .map(addr -> new InetSocketAddress(addr.getHostName(), addr.getPort() + 2))
                                    .collect(Collectors.toCollection(ArrayList::new));
                            this.leaderThread = new RoundRobinLeader(
                                    this,
                                    this.tcpPort,
                                    workerTCPPorts,
                                    this.cachedWork);
                            this.leaderThread.start();
                            this.cachedWork = null;
                        }
                    }
                    case OBSERVER -> {
                        if (this.currentLeader == null) {
                            this.logger.fine("Server is observing - starting search for leader");

                            // go through leader election as an observer
                            LeaderElection election = new LeaderElection(this, this.incomingMessages, this.logger);
                            setCurrentLeader(election.lookForLeader());

                            this.logger.fine("Election finished - leader elected: "
                                    + this.currentLeader.getProposedLeaderID());

                            // set up gossip
                            this.gossiper = new Gossiper(
                                    this,
                                    new HashMap<>(this.peerIDtoAddress),
                                    this.incomingMessages,
                                    this.gossipLogger,
                                    this.summary,
                                    this.verbose);
                            this.gossiper.start();
                        }
                    }
                    default -> throw new IllegalArgumentException("Unexpected value: " + getPeerState());
                }
            }
        } catch (Exception e) {
            this.logger.severe("Problem running in primary thread:\n" + e.getLocalizedMessage());

            this.UDPMessageSender.shutdown();
            this.UDPMessageReceiver.shutdown();
            this.logHost.stop(0);
            if (this.gossiper != null) {
                this.gossiper.shutdown();
            }
            if (this.followerThread != null) {
                this.followerThread.shutdown();
            }
            if (this.leaderThread != null) {
                this.leaderThread.shutdown();
            }
        }

        this.logger.severe("Shutdown complete");
    }

    @Override
    public void reportFailedPeer(long peerID) {
        this.logger.info("Server " + peerID + " failed");
        String summaryMessage = getServerId() + ": no heartbeat from server " + peerID + " - SERVER FAILED";
        this.summary.info(summaryMessage);
        System.out.println(summaryMessage);

        // clean up references to the failed server
        this.peerIDtoAddress.remove(peerID, this.peerIDtoAddress.get(peerID));

        // check if there is a leader
        if (this.currentLeader == null) {
            return;
        }
        // check if leader failed
        else if (this.currentLeader.getProposedLeaderID() == peerID) {
            this.logger.severe("Leader failed");

            // stop the gossiper
            this.gossiper.shutdown();
            this.gossiper = null;

            // observer only observers the failure
            if (this.state.equals(ServerState.OBSERVER)) {
                this.peerEpoch.incrementAndGet();
                this.currentLeader = null;
                return;
            }
            // workers need to deal with failed leader
            else {
                String switchMessage = getServerId()
                        + ": switching from "
                        + getPeerState()
                        + " to "
                        + ServerState.LOOKING;
                this.summary.info(switchMessage);
                System.out.println(switchMessage);

                // wait for cache, stop follower thread, then set up for election
                this.cachedWork = this.followerThread.awaitAndGrabCache();
                this.followerThread.shutdown();
                this.peerEpoch.incrementAndGet();
                this.currentLeader = null;
                this.state = ServerState.LOOKING;

                this.logger.info("Cache checked, follower stopped, epoch increased, and set for looking");
            }
        }
        // check if a worker failed (as leader)
        else if (this.currentLeader.getProposedLeaderID() == peerID) {
            this.logger.info("Worker failed");
        }
    }

    /**
     * Returns whether or not this server is busy working.
     * 
     * @return true iff this is a follower node that is currently busy
     */
    public boolean isWorking() {
        if (this.followerThread != null) {
            return this.followerThread.isWorking();
        }
        return false;
    }

    @Override
    public boolean isPeerDead(long peerID) {
        return !this.peerIDtoAddress.keySet().contains(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        for (Entry<Long, InetSocketAddress> entry : this.peerIDtoAddress.entrySet()) {
            if (entry.getValue().equals(address)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.udpPort;
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        // check that the target is not null and is in the cluster
        if (target == null || !this.peerIDtoAddress.containsValue(target)) {
            throw new IllegalArgumentException();
        }

        // put message on sending queue
        this.outgoingMessages.offer(
                new Message(
                        type,
                        messageContents,
                        this.myAddress.getHostString(),
                        this.udpPort,
                        target.getHostString(),
                        target.getPort()));
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for (InetSocketAddress target : this.peerIDtoAddress.values()) {
            if (target != null) {
                sendMessage(type, messageContents, target);
            }
        }
    }

    @Override
    public Long getServerId() {
        return this.serverID;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch.get();
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    /**
     * Calculate how many vote are requited to pass leader election. Start with the
     * id map size and add one to include this server. Divide the total in half and
     * add one to find the minimum threshold required to pass.
     * 
     * @return the quorum size required to pass leader election
     */
    @Override
    public int getQuorumSize() {
        return ((this.peerIDtoAddress.size() - this.numberOfObservers + 1) / 2) + 1;
    }

    /**
     * Creates an HttpHandler for sending log files on request.
     * 
     * @param log
     * @param id
     * @return an HttpHandler which sends log file contents upon request
     */
    private HttpHandler createLogHandler(String log, long id) {
        // find the log directory
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String dirName = "logs-" + date.format(formatter);
        File logDir = new File(dirName);
        if (!logDir.exists()) {
            throw new IllegalStateException("Log folder missing");
        }

        // get the log file
        File logFile = new File(dirName + File.separatorChar + this.serverType + "-" + id + "-" + log + "-Log.txt");
        if (!logFile.exists()) {
            throw new IllegalStateException("Log file for " + log + " missing");
        }

        return new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                byte[] response = Files.readAllBytes(logFile.toPath());
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
                exchange.getResponseBody().flush();
                exchange.getResponseBody().close();
            }
        };
    }

    /**
     * Class for keeping track of heartbeat values in gossip.
     */
    private static class Heartbeat {

        private long heartbeatCount;
        private long timestamp;

        private Heartbeat(long heartbeatCount, long timestamp) {
            this.heartbeatCount = heartbeatCount;
            this.timestamp = timestamp;
        }

        private Heartbeat() {
            this.heartbeatCount = 0;
            this.timestamp = System.currentTimeMillis();
        }

        private void beat() {
            this.heartbeatCount++;
        }

        private void setBeat(long newBeat) {
            this.heartbeatCount = newBeat;
        }

        private void setTime(long newTime) {
            this.timestamp = newTime;
        }
    }

    /**
     * Class for handling all gossip on a separate thread.
     */
    private static class Gossiper extends Thread implements LoggingServer {

        static final int GOSSIP = 1000;
        static final int FAIL = GOSSIP * 30;
        static final int CLEANUP = FAIL * 2;

        private final Logger logger;
        private final Logger summary;
        private final Logger verbose;
        private final PeerServerImpl server;
        private final LinkedBlockingQueue<Message> incomingMessages;
        private final Map<Long, InetSocketAddress> gossipIDtoAddress;
        private final long serverID;
        private final Map<Long, Heartbeat> heartbeatMap;
        private final Queue<Long> failedServers;
        private final Queue<Long> gossipQueue;

        private Gossiper(
                PeerServerImpl server,
                Map<Long, InetSocketAddress> gossipIDtoAddress,
                LinkedBlockingQueue<Message> incomingMessages,
                Logger logger,
                Logger summary,
                Logger verbose)
                throws IOException {

            setDaemon(true);
            setName("Gossiper-" + server.getServerId());
            this.logger = logger;
            this.logger.info("Constructing");

            // set up vars
            this.summary = summary;
            this.verbose = verbose;
            this.server = server;
            this.gossipIDtoAddress = gossipIDtoAddress;
            this.incomingMessages = incomingMessages;
            this.serverID = server.getServerId();
            this.heartbeatMap = new HashMap<Long, Heartbeat>();
            this.failedServers = new LinkedList<Long>();
            this.gossipQueue = new LinkedList<Long>(gossipIDtoAddress.keySet());
            Collections.shuffle((LinkedList<Long>) this.gossipQueue);

            // set up heartbeat map
            this.heartbeatMap.put(this.serverID, new Heartbeat());
        }

        @Override
        public void run() {
            this.logger.info("Running");

            // keep track of time for gossip
            long timer = System.currentTimeMillis();
            while (!isInterrupted()) {
                // check for all out failure
                if (this.gossipIDtoAddress.size() == 0) {
                    this.logger.severe("All out failure - all others failed");
                    break;
                }

                // gossip
                if (this.server.getPeerState() != ServerState.LOOKING && this.server.getCurrentLeader() != null) {
                    long currentTime;
                    if ((currentTime = System.currentTimeMillis()) - timer >= GOSSIP) {
                        this.heartbeatMap.get(this.serverID).beat();
                        gossip();
                        receiveGossip(currentTime);
                        timer = currentTime;
                    }
                    checkVitals(currentTime);
                }
            }

            this.logger.severe("Shutting down");
        }

        public void shutdown() {
            this.logger.info("Shutdown called");
            interrupt();
        }

        /**
         * Send out a gossip message to a random peer.
         */
        private void gossip() {
            // pick a random server to gossip with
            Long serverID = this.gossipQueue.poll();
            if (serverID == null) {
                this.gossipQueue.addAll(gossipIDtoAddress.keySet());
                Collections.shuffle((LinkedList<Long>) this.gossipQueue);
                serverID = this.gossipQueue.poll();
            }

            // send gossip to server
            this.server.sendMessage(MessageType.GOSSIP,
                    serializeGossip(this.heartbeatMap),
                    this.gossipIDtoAddress.get(serverID));

            this.logger.fine("Gossip sent to server: " + serverID);
        }

        /**
         * Check if gossip has been received. Compare heartbeat maps and update
         * accordingly. Additionally, check if any peers have failed or need to be
         * cleaned up.
         * 
         * @param currentTime
         */
        private void receiveGossip(long currentTime) {
            Message incomingGossip = this.incomingMessages.poll();
            if (incomingGossip == null) {
                return;
            } else if (incomingGossip.getMessageType() != MessageType.GOSSIP) {
                this.incomingMessages.add(incomingGossip);
                return;
            }

            // find the sender
            final long senderID = findSender(incomingGossip);
            if (senderID == -1) {
                return;
            }

            StringBuilder verboseMessage = new StringBuilder(256);
            verboseMessage.append("Gossip Received\n");
            verboseMessage.append("Time: " + currentTime + "\n");
            verboseMessage.append("Sender: server " + senderID + "\n");
            verboseMessage.append("Gossip content:\n(ID | Heartbeat Count)\n");

            // get the heartbeat map
            Map<Long, Long> gossipMap = deserializeGossip(incomingGossip.getMessageContents());

            // update this servers map and check if any of the servers are timed out
            for (Long id : gossipMap.keySet()) {
                this.heartbeatMap.compute(id, (currentKey, currentHeartbeat) -> {
                    Long gossipHeartbeatCount = gossipMap.get(id);
                    verboseMessage.append(id + " | " + gossipHeartbeatCount + "\n");
                    if (currentHeartbeat == null) {
                        this.summary.info(this.serverID
                                + ": added "
                                + id
                                + " with heartbeat sequence "
                                + gossipHeartbeatCount
                                + " based on message from "
                                + senderID
                                + " at node time "
                                + currentTime);
                        return new Heartbeat(gossipHeartbeatCount, currentTime);
                    } else if (currentHeartbeat.heartbeatCount < gossipHeartbeatCount) {
                        currentHeartbeat.setBeat(gossipHeartbeatCount);
                        currentHeartbeat.setTime(currentTime);
                        this.summary.info(this.serverID
                                + ": updated "
                                + id
                                + "'s heartbeat sequence to "
                                + gossipHeartbeatCount
                                + " based on message from "
                                + senderID
                                + " at node time "
                                + currentTime);
                    }
                    return currentHeartbeat;
                });
            }

            this.verbose.info(verboseMessage.toString());
        }

        /**
         * Find and return the id of the server who sent the gossip.
         * 
         * @param incomingGossip
         * @return the id of the sender or -1 if not found
         */
        private long findSender(Message incomingGossip) {
            for (Entry<Long, InetSocketAddress> entry : this.gossipIDtoAddress.entrySet()) {
                if (entry.getValue().getPort() == incomingGossip.getSenderPort()) {
                    this.logger.fine("Received gossip from server: " + entry.getKey());
                    return entry.getKey();
                }
            }
            return -1;
        }

        /**
         * Check each nodes vitals. Mark off failed node, and clean up expired nodes.
         * 
         * @param currentTime
         */
        private void checkVitals(long currentTime) {
            Heartbeat vitals;

            // check if any nodes have failed
            int idsSize = this.gossipIDtoAddress.size();
            Long[] ids = this.gossipIDtoAddress.keySet().toArray(new Long[idsSize]);
            for (long id : ids) {
                vitals = this.heartbeatMap.get(id);
                if (vitals != null && currentTime - vitals.timestamp >= FAIL) {
                    this.logger.info("Server " + id + " has failed");
                    this.gossipQueue.remove(id);
                    this.gossipIDtoAddress.remove(id);
                    this.failedServers.add(id);
                    this.server.reportFailedPeer(id);
                }
            }

            // check if any nodes need to be cleaned up
            Long failedID;
            while ((failedID = this.failedServers.peek()) != null) {
                vitals = this.heartbeatMap.get(failedID);
                if (currentTime - vitals.timestamp >= CLEANUP) {
                    this.logger.info("Server " + failedID + " reached clean up");
                    this.failedServers.poll();
                    this.failedServers.remove(failedID);
                    this.heartbeatMap.remove(failedID);
                } else {
                    break;
                }
            }
        }

        /**
         * Serialize a heartbeat map into a byte array.
         * 
         * @param heartbeatMap
         * @return gossip in byte[] form
         */
        private static byte[] serializeGossip(Map<Long, Heartbeat> heartbeatMap) {
            byte[] gossip = new byte[heartbeatMap.size() * 16];
            ByteBuffer buffer = ByteBuffer.wrap(gossip);
            buffer.clear();
            for (Map.Entry<Long, Heartbeat> entry : heartbeatMap.entrySet()) {
                buffer.putLong(entry.getKey());
                buffer.putLong(entry.getValue().heartbeatCount);
            }
            return gossip;
        }

        /**
         * Deserialize a byte array into a heartbeat map.
         * 
         * @param messageContents
         * @return gossip in Map form
         */
        private static Map<Long, Long> deserializeGossip(byte[] messageContents) {
            Map<Long, Long> gossip = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.wrap(messageContents);
            for (int i = 0; i < messageContents.length / 16; i++) {
                gossip.put(buffer.getLong(), buffer.getLong());
            }
            return gossip;
        }
    }
}
