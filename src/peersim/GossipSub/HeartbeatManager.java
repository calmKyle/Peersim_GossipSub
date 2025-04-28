package peersim.GossipSub;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;

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
//    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);
     private boolean isDEBUG = true;

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

//        for (Map.Entry<BigInteger, PeerScoreInfo> entry : peerScores.entrySet()) {
//            PeerScoreInfo psi = entry.getValue();
//            decayPeerTopicCounters(psi); // <-- new
//            double oldScore = psi.cachedScore;
//            double newScore = protocol.computeScore(psi);
//            psi.cachedScore = newScore;
//
//            if (isDEBUG) {
//                System.out.println("[DEBUG SCORE] Peer=" + entry.getKey() +
//                        " oldScore=" + oldScore +
//                        " newScore=" + newScore);
//            }
//        }

        for (Map.Entry<BigInteger, PeerScoreInfo> entry
                : new ArrayList<>(peerScores.entrySet())) {
            BigInteger      peerID = entry.getKey();
            PeerScoreInfo   psi    = entry.getValue();

            decayPeerTopicCounters(psi);
            double oldScore = psi.cachedScore;
            double newScore = protocol.computeScore(psi);
            psi.cachedScore = newScore;

            if (isDEBUG) {
                System.out.printf("[SCORE] peer=%s  old=%.2f  new=%.2f%n",
                        peerID, oldScore, newScore);
            }

            /* ---------- apply threshold gates ---------- */
            if (newScore < GossipScoringConfig.GRAYLIST_THRESHOLD) {
                /* hard gray-list: drop all RPC from this peer */
//                protocol.graylistPeer(peerID);
                continue;  // nothing else to do with a gray-listed peer
            }

            if (newScore < GossipScoringConfig.PUBLISH_THRESHOLD) {
                /* below publishThreshold → prune from every mesh */
                for (String topicID : protocol.localMesh.keySet()) {
                    if (protocol.inMyMesh(topicID, peerID)) {
                        protocol.prunePeer(peerID, topicID);
                    }
                }
            }

            if (newScore < GossipScoringConfig.GOSSIP_THRESHOLD) {
                /*  gossipThreshold → ignore IHAVE/IWANT from this peer */
//                protocol.blackholeGossip(peerID);
            }

            if (newScore > GossipScoringConfig.ACCEPTPX_THRESHOLD) {
                /* high score → send Peer-Exchange */
//                protocol.maybeSharePeerExchange(peerID);
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
        for (Map.Entry<String, PeerScoreInfo.TopicScores> e : psi.topicScoresMap.entrySet()) {
            String t = e.getKey();
            PeerScoreInfo.TopicScores ts = e.getValue();
            GossipScoringConfig.TopicParam p =
                    GossipScoringConfig.TOPIC_PARAMS.getOrDefault(
                            t, GossipScoringConfig.DEFAULT_TOPIC_PARAM);

            ts.firstMessageDeliveries = ts.firstMessageDeliveries * p.firstMsgDecay;
            ts.invalidMessages        = ts.invalidMessages  * p.invalidMsgDecay;
            ts.meshMsgDelivered       = ts.meshMsgDelivered * p.meshDeliveriesDecay;

            /* expected messages grows each heartbeat by the threshold */
            ts.meshMsgExpected = ts.meshMsgExpected * p.meshDeliveriesDecay
                    + p.meshDeliveriesThreshold;
            ts.underDelivery   = Math.max(0,
                    ts.meshMsgExpected - ts.meshMsgDelivered);
        }
    }


}
