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
        public int firstMessageDeliveries; // # times peer is first to deliver
        public int invalidMessages;        // # invalid messages from peer
        public int meshMsgDelivered;       // actual # of messages delivered
        public int meshMsgExpected;        // how many we "expect" them to deliver
        public int underDelivery;          // shortfall between expected vs. delivered

        public TopicScores() {
            this.firstMessageDeliveries = 0;
            this.invalidMessages = 0;
            this.meshMsgDelivered = 0;
            this.meshMsgExpected = 10; // Example default
            this.underDelivery = 0;
        }
    }
}
