// package peersim.GossipSub.PANDAS;

// import java.math.BigInteger;
// import java.util.Random;

// import peersim.GossipSub.CustomDistribution;
// import peersim.GossipSub.GossipSubProtocol;
// import peersim.GossipSub.Message;
// import peersim.GossipSub.Topic;
// import peersim.config.Configuration;
// import peersim.core.Node;

// public class SamplingManager {
// private GossipSubProtocol protocol;
// private Random random = new Random();

// public SamplingManager(GossipSubProtocol protocol) {
// this.protocol = protocol;
// }

// /**
// * Initiates a random sample request following PANDAS' principles.
// */
// public void sampleDataRequest() {
// boolean isRow = random.nextBoolean();
// int randomSampleIndex =
// random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512));
// int topicNo = randomSampleIndex /
// (Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC", 16) / 2);
// Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo +
// 1));

// if (topicToSubscribe == null)
// return;

// protocol.subscribeTopic(topicToSubscribe);

// int nodeIndex = getNodeIndex(isRow, randomSampleIndex);
// BigInteger destId = PeerDiscovery.getNodeIdFromTopic(topicToSubscribe,
// nodeIndex, isRow, protocol.gossipSubId);

// if (destId == null || destId.equals(protocol.nodeId)) {
// sampleDataRequest(); // Retry if invalid
// return;
// }

// // Create sampling request message
// Message sampleReqMes = protocol.createMessage(
// -1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID,
// Integer.toString(randomSampleIndex), isRow, randomSampleIndex, -1, 0, -1);

// // Track request
// protocol.sentMsg.put(sampleReqMes.id, sampleReqMes);
// protocol.NoOfSampleRequestsSent++;

// // Publish message
// protocol.publishMessage(sampleReqMes, destId, protocol.gossipSubId);

// // Set timeout for retry
// protocol.setSampleTimeout(destId, sampleReqMes);
// }

// private int getNodeIndex(boolean isRow, int randomSampleIndex) {
// if (protocol.distributionStrategy == 1) {
// int sampleHolderNodeIdx = random.nextInt(8);
// return ((randomSampleIndex % 8) * 8) + sampleHolderNodeIdx;
// } else {
// return random.nextInt(64);
// }
// }

// /**
// * Start multiple sampling requests as per PANDAS guidelines.
// */
// public void startSampling() {
// for (int i = 0; i < 73; i++) { // PANDAS suggests 73 samples per validator
// per round
// sampleDataRequest();
// }
// }
// }
