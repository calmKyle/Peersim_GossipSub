package peersim.GossipSub;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Revised PeerScoreInfo that tracks counters per topic.
 */
public class PeerScoreInfo {

    // Time when the peer joined the mesh (used for "time in mesh" scoring).
    public long timeInMeshStart;

    private final Map<String, TopicScores> topics = new HashMap<>();


    // Last computed overall (aggregated) score, for quick lookup.
    public double cachedScore;

    // We can remember when the peer connected, if needed.
    public long connectedTime;

    // Holds per-topic stats so we can penalize or reward the peer specifically
    // for each topic. Key = topicID, value = container of counters for that topic.
    public Map<String, TopicScores> topicScoresMap;

    public long pruneBackoffUntil = 0L;

    private final Map<BigInteger, Long> pruneBackoffUntilByTopic = new HashMap<>();

    public void setPruneBackoffForTopic(BigInteger topicId, long untilMillis) {
        if (topicId == null) return;
        pruneBackoffUntilByTopic.put(topicId, untilMillis);
    }

    public boolean isPruneBackoffActiveForTopic(String topicId, long nowMillis) {
        if (topicId == null) return false;
        Long until = pruneBackoffUntilByTopic.get(topicId);
        return until != null && nowMillis < until;
    }

    public void purgeExpiredPruneBackoff(long nowMillis) {
        if (pruneBackoffUntilByTopic.isEmpty()) return;
        pruneBackoffUntilByTopic.entrySet().removeIf(e -> e.getValue() <= nowMillis);
    }

    public double appScore   = 0.0;   // P5: application-specific trust
    public double ipSurplus  = 0.0;   // P6: (peersOnThisIP – threshold)²

    // We record the last time we updated the counters to handle time-based decays.
    public long lastUpdateTime;

    public TopicScores getOrCreate(String topic) {
        return topics.computeIfAbsent(topic, t -> new TopicScores());
    }

    public void onGraft(String topic, long nowMs) {
        TopicScores ts = getOrCreate(topic);
        ts.timeInMeshStart = nowMs;           // set when entering the mesh
    }

    public void onPrune(String topic) {
        TopicScores ts = getOrCreate(topic);
        ts.timeInMeshStart = -1L;             //  clear when leaving the mesh
    }

    public long topicTimeInMeshMs(String topic, long nowMs) {
        TopicScores ts = topics.get(topic);
        if (ts == null || ts.timeInMeshStart < 0) return 0L;
        return Math.max(0L, nowMs - ts.timeInMeshStart);
    }

    public Map<String, TopicScores> getTopicsView() {
        return Collections.unmodifiableMap(topics);
    }

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
        public long pruneBackoffUntil;
        public long timeInMeshStart;

        public TopicScores() {
            long timeInMeshStart = -1L;
            firstMessageDeliveries = 0;
            invalidMessages = 0;
            meshMsgDelivered = 0;
            meshMsgExpected = 10;
            underDelivery = 0;
        }
    }
}
