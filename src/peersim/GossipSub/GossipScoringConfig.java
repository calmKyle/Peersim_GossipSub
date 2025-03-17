// File: GossipScoringConfig.java (a new file)
package peersim.GossipSub;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds per-topic scoring parameters, mirroring the structure from
 * gossip_scoring_params.go.
 */
public class GossipScoringConfig {

    public static class TopicParam {
        public double topicWeight;
        public double timeInMeshWeight;
        public double timeInMeshCap;

        public double firstMessageDeliveriesWeight;
        public double firstMessageDeliveriesDecay;
        public double firstMessageDeliveriesCap;

        public double meshMessageDeliveriesWeight;
        public double meshMessageDeliveriesDecay;
        public double meshMessageDeliveriesCap;
        public double meshMessageDeliveriesThreshold;

        public double invalidMessageDeliveriesWeight;
        public double invalidMessageDeliveriesDecay;

        // (e.g. penalty weights, etc.)
    }

    public static final Map<String, TopicParam> TOPIC_PARAMS = new HashMap<>();

    static {
        // Example: "Beacon block" topic (mirroring defaultBlockTopicParams)
        {
            TopicParam blockParam = new TopicParam();
            blockParam.topicWeight = 0.8; // from beaconBlockWeight
            blockParam.timeInMeshWeight = 0.05; // example, pick your own
            blockParam.timeInMeshCap = 60000; // e.g. cap on time in mesh

            blockParam.firstMessageDeliveriesWeight = 1.0;
            blockParam.firstMessageDeliveriesDecay = 0.9; // approximate
            blockParam.firstMessageDeliveriesCap = 23; // from go code

            blockParam.meshMessageDeliveriesWeight = -0.717; // from go code
            blockParam.meshMessageDeliveriesDecay = 0.9;
            blockParam.meshMessageDeliveriesCap = 64; // e.g. some limit
            blockParam.meshMessageDeliveriesThreshold = 6.4; // e.g. 1/10 of cap

            blockParam.invalidMessageDeliveriesWeight = -140.4475;
            blockParam.invalidMessageDeliveriesDecay = 0.99;

            TOPIC_PARAMS.put("beacon_block_topic", blockParam);
        }

        // Example: "Aggregate" topic
        {
            TopicParam aggParam = new TopicParam();
            aggParam.topicWeight = 0.5; // from aggregateWeight
            // fill out the rest (decay, caps) similarly
            TOPIC_PARAMS.put("aggregate_topic", aggParam);
        }

        // etc. for each topic from gossip_scoring_params.go
    }

    // Example thresholds, if you want them:
    public static double GOSSIP_THRESHOLD = -4000;
    public static double PUBLISH_THRESHOLD = -8000;
    public static double GRAYLIST_THRESHOLD = -16000;
    public static double ACCEPTPX_THRESHOLD = 100;
    // etc.
}
