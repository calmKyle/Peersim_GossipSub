package peersim.GossipSub;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import peersim.config.Configuration;
import peersim.core.CommonState;

public class HeartbeatManager {

    // These constants come from the original GossipSubProtocol
    private static final long MESSAGE_EXPIRATION_MS = 4001;
    private static final int GOSSIP_ADVERTISE_ROUNDS = 2;

    private static final double DECAY_FACTOR = 0.9;

    private Map<Long, EphemeralMsgInfo> ephemeralCache;
    private Map<BigInteger, PeerScoreInfo> peerScores;
    private GossipSubProtocol protocol;
    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);
    // private boolean isDEBUG = true;

    public HeartbeatManager(GossipSubProtocol protocol,
            Map<Long, EphemeralMsgInfo> ephemeralCache,
            Map<BigInteger, PeerScoreInfo> peerScores,
            boolean isDEBUG) {
        this.protocol = protocol;
        this.ephemeralCache = ephemeralCache;
        this.peerScores = peerScores;
        this.isDEBUG = isDEBUG;
    }

    public void runHeartbeat(int myPid) {
        long now = CommonState.getTime();

        if (isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] Heartbeat at t=" + now +
                    " Node=" + protocol.getNodeId() +
                    " ephemeralCacheSize=" + ephemeralCache.size());
        }

        // 1) Expire old ephemeral messages
        expireEphemeralMessages(now);

        // 2) Re-advertise (IHAVE) messages up to GOSSIP_ADVERTISE_ROUNDS times
        reAdvertiseEphemeral(myPid);

        // 3) Update peer scores & prune/graft if needed
        if (isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] Computing peer scores on node " + protocol.getNodeId());
        }

        for (Map.Entry<BigInteger, PeerScoreInfo> entry : peerScores.entrySet()) {
            PeerScoreInfo psi = entry.getValue();
            decayPeerTopicCounters(psi); // <-- new
            double oldScore = psi.cachedScore;
            double newScore = protocol.computeScore(psi);
            psi.cachedScore = newScore;

            if (isDEBUG) {
                System.out.println("[DEBUG SCORE] Peer=" + entry.getKey() +
                        " oldScore=" + oldScore +
                        " newScore=" + newScore);
            }
        }

        // Example: prune peers with negative score
        // for (BigInteger peerID : new HashSet<>(peerScores.keySet())) {
        // PeerScoreInfo psi = peerScores.get(peerID);
        // if (psi.cachedScore < 0) {
        // if (isDEBUG) {
        // System.out.println("[DEBUG HEARTBEAT] removing peer " + peerID +
        // " from mesh due to negative score on node " + protocol.getNodeId());
        // }
        // protocol.removePeerFromMesh(peerID);
        // }
        // }

        for (BigInteger peerID : peerScores.keySet()) {
            PeerScoreInfo psi = peerScores.get(peerID);
            double s = protocol.computeScore(psi);
            if (s < 0) {
                // for each topic where this peer is in your mesh, prune them
                for (String topicID : protocol.localMesh.keySet()) {
                    Set<BigInteger> set = protocol.localMesh.get(topicID);
                    if (set != null && set.contains(peerID)) {
                        System.out.println("[DEBUG HEARTBEAT] Node " + protocol.nodeId
                                + " => Score of peer " + peerID + " is " + s
                                + "; will prune from mesh.");
                        protocol.prunePeer(peerID, topicID);
                    }
                }
            }
        }

        // Update mesh connections if needed
        protocol.updateMeshConnections();
    }

    private void expireEphemeralMessages(long now) {
        Iterator<Entry<Long, EphemeralMsgInfo>> it = ephemeralCache.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, EphemeralMsgInfo> entry = it.next();
            EphemeralMsgInfo info = entry.getValue();
            if (now - info.arrivalTime >= MESSAGE_EXPIRATION_MS) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Expiring msg " + entry.getKey() +
                            " from ephemeralCache on node " + protocol.getNodeId());
                }
                it.remove();
            }
        }
    }

    private void reAdvertiseEphemeral(int myPid) {
        for (EphemeralMsgInfo info : ephemeralCache.values()) {
            if (info.advertiseCount < GOSSIP_ADVERTISE_ROUNDS) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Re-advertising msgID=" + info.message.id +
                            " node=" + protocol.getNodeId() +
                            " advCount=" + info.advertiseCount);
                }
                protocol.advertiseMessageIHAVE(info.message, myPid);
                info.advertiseCount++;
            }
        }
    }

    /**
     * Decays each peer’s counters on each topic by DECAY_FACTOR.
     */
    private void decayPeerTopicCounters(PeerScoreInfo psi) {
        for (PeerScoreInfo.TopicScores tsc : psi.topicScoresMap.values()) {
            tsc.firstMessageDeliveries = (int) Math.floor(tsc.firstMessageDeliveries * DECAY_FACTOR);
            tsc.invalidMessages = (int) Math.floor(tsc.invalidMessages * DECAY_FACTOR);
            tsc.meshMsgDelivered = (int) Math.floor(tsc.meshMsgDelivered * DECAY_FACTOR);
            // tsc.meshMsgExpected could be left alone or decayed if you prefer
            // tsc.underDelivery is recalculated each computeScore
        }
    }

}
