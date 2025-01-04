package com.maxfdev.projects;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.maxfdev.projects.Message.MessageType;
import com.maxfdev.projects.PeerServer.ServerState;

/**
 * We are implementing a simplified version of the election algorithm. For the
 * complete version which covers all possible scenarios, see
 * https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 2000;

    /**
     * Upper bound on the amount of time between two consecutive notification
     * checks.
     * This impacts the amount of time to get the system up again after long
     * partitions. Currently 3 seconds.
     */
    private final static int maxNotificationInterval = 1500;

    // globals
    private final PeerServer server;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Logger logger;
    private final boolean isWorker;
    private long proposedEpoch;
    private long proposedLeader;

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        logger.info("Constructing leader election");

        // set globals
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.logger = logger;
        this.isWorker = !server.getPeerState().equals(ServerState.OBSERVER);
        if (this.isWorker) {
            this.proposedEpoch = server.getPeerEpoch();
            this.proposedLeader = server.getServerId();
        } else {
            this.proposedEpoch = 0l;
            this.proposedLeader = 0l;
        }
    }

    /**
     * Note that the logic in the comments below does NOT cover every last
     * "technical" detail you will need to address to implement the election
     * algorithm.
     * How you store all the relevant state, etc., are details you will need to work
     * out.
     * 
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        try {
            this.logger.info("Starting look for leader");

            // send initial notifications to get things started
            if (this.isWorker) {
                sendNotifications();
            }

            // set a notification interval
            int notificationInterval = 10;

            // create map to keep track of votes
            Map<Long, ElectionNotification> votes = new HashMap<Long, ElectionNotification>(
                    this.server.getQuorumSize());

            // Loop in which we exchange notifications with other servers until we find a
            // leader
            while (true) {
                // Remove next notification from queue
                Message nextNotification = this.incomingMessages.poll(notificationInterval, TimeUnit.MILLISECONDS);

                // If no notifications received...
                if (nextNotification == null) {
                    // ...resend notifications to prompt a reply from others
                    if (this.isWorker) {
                        sendNotifications();
                    }

                    // ...use exponential back-off when notifications not received but no longer
                    // than maxNotificationInterval...
                    notificationInterval = Math.min(
                            notificationInterval << 1,
                            LeaderElection.maxNotificationInterval);
                }
                // If we did get a message...
                else if (nextNotification.getMessageType().equals(MessageType.ELECTION)) {
                    // get the vote
                    ElectionNotification vote = getNotificationFromMessage(nextNotification);

                    // ...if it's for an earlier epoch, or from an observer, ignore it.
                    if (checkInvalidVote(vote)) {
                        continue;
                    }

                    // ...if the received message has a vote for a leader which supersedes mine,
                    // change my vote (and send notifications to all other voters about my new
                    // vote).
                    if (supersedesCurrentVote(vote.getProposedLeaderID(), vote.getPeerEpoch())) {
                        updateVote(vote);
                    }

                    // (Be sure to keep track of the votes I received and who I received them from.)
                    votes.put(vote.getSenderID(), vote);

                    // If I have enough votes to declare my currently proposed leader as the
                    // leader...
                    if (haveEnoughVotes(votes, new Vote(this.proposedLeader, this.proposedEpoch))) {
                        boolean end = true;

                        // ..do a last check to see if there are any new votes for a higher ranked
                        // possible leader. If there are, continue in my election "while" loop.
                        while ((nextNotification = this.incomingMessages.poll(LeaderElection.finalizeWait,
                                TimeUnit.MILLISECONDS)) != null) {
                            // throw away nonelection notifications
                            if (!nextNotification.getMessageType().equals(MessageType.ELECTION)) {
                                continue;
                            }

                            // update vote
                            vote = getNotificationFromMessage(nextNotification);

                            // check if vote is valid
                            if (checkInvalidVote(vote)) {
                                continue;
                            }

                            // check if vote supersedes the current one
                            if (supersedesCurrentVote(vote.getProposedLeaderID(), vote.getPeerEpoch())) {
                                updateVote(vote);
                                end = false;
                                break;
                            }

                            // keep track of vote
                            votes.put(vote.getSenderID(), vote);
                        }

                        // If there are no new relevant message from the reception queue, set my own
                        // state to either LEADING or FOLLOWING and RETURN the elected leader.
                        if (end) {
                            return acceptElectionWinner(new ElectionNotification(
                                    this.proposedLeader,
                                    this.server.getPeerState(),
                                    this.server.getServerId(),
                                    this.proposedEpoch));
                        }
                    }
                }
            }
        } catch (Exception e) {
            this.logger.log(Level.SEVERE, "Exception occurred during election; election canceled", e);
        }
        return null;
    }

    /**
     * Check if this vote is valid or should be ignored.
     * 
     * @param vote
     * @return true if the vote is invalid
     */
    private boolean checkInvalidVote(ElectionNotification vote) {
        return (vote.getPeerEpoch() < this.proposedEpoch ||
                vote.getState() == null ||
                vote.getState().equals(ServerState.OBSERVER));
    }

    /**
     * Update the globals for who the server is voting for and send out updated
     * notification.
     * 
     * @param vote
     */
    private void updateVote(ElectionNotification vote) {
        // change proposed epoch and leader
        this.proposedEpoch = vote.getPeerEpoch();
        this.proposedLeader = vote.getProposedLeaderID();

        // send updated notifications
        if (this.isWorker) {
            sendNotifications();
        }
    }

    /**
     * Accept the election winner by set the peer server state based on the
     * notification. If this server is the winner than set its state to leading
     * (otherwise following). Additionally clear the incoming message queue and
     * return the notification as a vote.
     * 
     * @param n
     * @return the election notification as a vote
     */
    private Vote acceptElectionWinner(ElectionNotification n) {
        if (this.isWorker) {
            // get updated state
            ServerState updatedState = n.getProposedLeaderID() == this.server.getServerId()
                    ? ServerState.LEADING
                    : ServerState.FOLLOWING;

            this.logger.info("Server accepting state: " +
                    updatedState.toString() +
                    "\nWith leader: " +
                    n.getProposedLeaderID());

            // set my state to either LEADING or FOLLOWING
            this.server.setPeerState(updatedState);
        } else {
            this.logger.info("Observation: quorum formed with leader ID " + n.getProposedLeaderID());
        }

        // clear out the incoming queue before returning
        this.incomingMessages.clear();

        // return the election notification
        return new Vote(n.getProposedLeaderID(), n.getPeerEpoch());
    }

    /**
     * We return true if one of the following two cases hold:
     * <ol>
     * <li>New epoch is higher</li>
     * <li>New epoch is the same as current epoch, but server id is higher.</li>
     * </ol>
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient
     * support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one
     * current vote.
     * 
     * <p>
     * Is the number of votes for the proposal > the size of my peer server’s
     * quorum?
     * </p>
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        // keep count of all common votes
        int voteCount = this.isWorker ? 1 : 0;

        // quantify the threshold
        int threshold = this.server.getQuorumSize();

        // add up common votes
        for (ElectionNotification vote : votes.values()) {
            if (vote.getProposedLeaderID() == proposal.getProposedLeaderID()) {
                voteCount++;
            }
        }

        return voteCount >= threshold;
    }

    /**
     * Send out election notifications to the broadcast list.
     */
    private void sendNotifications() {
        // make the election notification
        ElectionNotification notification = new ElectionNotification(
                this.proposedLeader,
                this.server.getPeerState(),
                this.server.getServerId(),
                this.proposedEpoch);

        // turn the notification into bytes
        byte[] notificationContent = buildMsgContent(notification);

        // broadcast the notification
        this.server.sendBroadcast(MessageType.ELECTION, notificationContent);
    }

    /**
     * Build the byte array for the message content in the following order:
     * <ol>
     * <li>Make a byte array to represent the data of size 26 (3 longs + 1
     * char)</li>
     * <li>Use a byte buffer to input the data</li>
     * <li>Add message content</li>
     * </ol>
     * 
     * @param notification
     * @return the byte[] for the message content
     */
    public static byte[] buildMsgContent(ElectionNotification notification) {
        byte[] notificationContent = new byte[26];

        // set up buffer
        ByteBuffer buffer = ByteBuffer.wrap(notificationContent);
        buffer.clear();

        // add sender state, sender id, proposed leader, epoch
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());

        return notificationContent;
    }

    /**
     * Get an election notification from a received message.
     * For how message content is built see
     * {@link #buildMsgContent(ElectionNotification)}.
     * 
     * @param received
     * @return the election notification
     */
    public static ElectionNotification getNotificationFromMessage(Message received) {
        byte[] notificationContent = received.getMessageContents();

        // set up a buffer to read the notification
        ByteBuffer buffer = ByteBuffer.wrap(notificationContent);

        // retrieve the notification data (state, sender id, proposed leader, epoch)
        long proposedID = buffer.getLong();
        ServerState state = ServerState.getServerState(buffer.getChar());
        long senderID = buffer.getLong();
        long peerEpoch = buffer.getLong();

        return new ElectionNotification(proposedID, state, senderID, peerEpoch);
    }
}
