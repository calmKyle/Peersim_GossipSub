package peersim.GossipSub;

import java.util.HashMap;
import java.util.Map;

/**
 * Revised PeerScoreInfo that tracks counters per topic.
 */
public class PeerScoreInfo {

    // Time when the peer joined the mesh (used for "time in mesh" scoring).
    public long timeInMeshStart;

    // Last computed overall (aggregated) score, for quick lookup.
    public double cachedScore;

    // We can remember when the peer connected, if needed.
    public long connectedTime;

    // Holds per-topic stats so we can penalize or reward the peer specifically
    // for each topic. Key = topicID, value = container of counters for that topic.
    public Map<String, TopicScores> topicScoresMap;

    public long pruneBackoffUntil = 0L;

    public double appScore   = 0.0;   // P5: application-specific trust
    public double ipSurplus  = 0.0;   // P6: (peersOnThisIP – threshold)²

    // We record the last time we updated the counters to handle time-based decays.
    public long lastUpdateTime;

    public PeerScoreInfo(long currentTime) {
        this.timeInMeshStart = currentTime;
        this.connectedTime = currentTime;
        this.cachedScore = 0.0;
        this.lastUpdateTime = currentTime;
        this.topicScoresMap = new HashMap<>();
    }

    /**
     * Helper class to hold all the counters relevant to scoring for a single topic.
     */
    public static class TopicScores {
        public double firstMessageDeliveries;
        public double invalidMessages;
        public double meshMsgDelivered;
        public double meshMsgExpected;
        public double underDelivery;

        public TopicScores() {
            firstMessageDeliveries = 0;
            invalidMessages = 0;
            meshMsgDelivered = 0;
            meshMsgExpected = 10;
            underDelivery = 0;
        }
    }
}
