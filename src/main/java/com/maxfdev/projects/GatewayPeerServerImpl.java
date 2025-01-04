package com.maxfdev.projects;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends PeerServerImpl {
    public GatewayPeerServerImpl(
            int udpPort,
            long peerEpoch,
            Long serverID,
            Map<Long, InetSocketAddress> peerIDtoAddress,
            Long gatewayID,
            int numberOfObservers)
            throws IOException {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers);
    }

    @Override
    public void setPeerState(ServerState newState) {
        // make sure state is never changed
    }

    /**
     * Get the leaders UDP address or null if leader doesn't exist currently or is
     * not in peer ID map.
     * 
     * @return leader's UDP InetSocketAddress or null
     */
    public InetSocketAddress getLeaderAddress() {
        Vote leaderVote = getCurrentLeader();
        if (leaderVote == null) {
            return null;
        } else {
            return getPeerByID(leaderVote.getProposedLeaderID());
        }
    }
}
