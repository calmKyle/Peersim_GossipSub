package peersim.GossipSub;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.Map.Entry;

import peersim.config.Configuration;
import peersim.core.CommonState;

public class HeartbeatManager {

    // These constants come from the original GossipSubProtocol
    private static final long MESSAGE_EXPIRATION_MS = 4000;
    private static final int GOSSIP_ADVERTISE_ROUNDS = 1;

    private Map<Long, EphemeralMsgInfo> ephemeralCache;
    private Map<BigInteger, PeerScoreInfo> peerScores;
    private GossipSubProtocol protocol; // Reference to the parent protocol instance
    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

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
        Iterator<Entry<Long, EphemeralMsgInfo>> it = ephemeralCache.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, EphemeralMsgInfo> entry = it.next();
            EphemeralMsgInfo info = entry.getValue();
            if (now - info.arrivalTime >= MESSAGE_EXPIRATION_MS) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Expiring message " + entry.getKey() +
                            " from ephemeralCache on node " + protocol.getNodeId());
                }
                it.remove();
            }
        }

        // 2) Re-advertise (IHAVE) messages up to GOSSIP_ADVERTISE_ROUNDS times
        for (EphemeralMsgInfo info : ephemeralCache.values()) {
            if (info.advertiseCount < GOSSIP_ADVERTISE_ROUNDS) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Re-advertising messageID=" + info.message.id +
                            " advertiseCount=" + info.advertiseCount +
                            " node=" + protocol.getNodeId());
                }
                protocol.advertiseMessageIHAVE(info.message, myPid);
                info.advertiseCount++;
            }
        }

        // 3) Update peer scores & prune/graft if needed
        if (isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] Computing peer scores on node " + protocol.getNodeId());
        }

        for (Map.Entry<BigInteger, PeerScoreInfo> entry : peerScores.entrySet()) {
            PeerScoreInfo psi = entry.getValue();
            double newScore = protocol.computeScore(psi);
            double oldScore = psi.cachedScore;
            psi.cachedScore = newScore;

            if (isDEBUG) {
                System.out.println("[DEBUG SCORE]   Peer=" + entry.getKey() +
                        " oldScore=" + oldScore +
                        " newScore=" + newScore);
            }
        }

        // Example: prune peers with negative score
        for (BigInteger peerID : new HashSet<>(peerScores.keySet())) {
            PeerScoreInfo psi = peerScores.get(peerID);
            if (psi.cachedScore < 0) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Removing peer " + peerID +
                            " from mesh due to negative score on node " + protocol.getNodeId());
                }
                protocol.removePeerFromMesh(peerID);
            }
        }

        // Update mesh connections if needed
        protocol.updateMeshConnections();
    }

}
