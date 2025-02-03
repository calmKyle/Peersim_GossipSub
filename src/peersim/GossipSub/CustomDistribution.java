package peersim.GossipSub;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

/**
 * Initializes the network by assigning unique NodeIds to each node
 * and setting up topics for the GossipSub protocol.
 */
public class CustomDistribution implements peersim.core.Control {

    private static final String PAR_PROT = "protocol";
    private static final int NUMBER_OF_TOPICS = Configuration.getInt("NUMBER_OF_TOPICS", 64);
    private static final int NUMBER_OF_VALIDATOR_NODES = Configuration.getInt("NUMBER_OF_VALIDATORS", 1024);
    private static final int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC");

    private final int gossipProtocolID;
    private final UniformRandomGenerator urg;

    public static Node blockProposerNode;
    public static final Map<BigInteger, Node> networkNodes = new LinkedHashMap<>();
    public static final Map<String, Topic> topics = new LinkedHashMap<>(NUMBER_OF_TOPICS);

    public String prefix;

    public CustomDistribution(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        this.urg = new UniformRandomGenerator(160, CommonState.r);

        this.prefix = prefix;
    }

    /**
     * Assigns a random NodeId to each node and initializes topics.
     */
    public boolean execute() {
        initializeNodes();
        initializeTopics();
        assignValidatorsToTopics();
        return false;
    }

    private void initializeNodes() {
        for (int i = 0; i < Network.size(); i++) {
            BigInteger nodeId = urg.generate();
            Node node = Network.get(i);

            ((GossipSubProtocol) node.getProtocol(gossipProtocolID)).setNodeId(nodeId);
            networkNodes.put(nodeId, node);

            if (i == 0)
                blockProposerNode = node; // Set the first node as block proposer
        }
    }

    public static void initializeTopics() {
        if (topics == null) {
            System.err.println("[ERROR] `topics` is not initialized! Check where it is declared.");
            return;
        }

        for (int i = 1; i <= NUMBER_OF_TOPICS; i++) {
            String topicKey = "Topic-" + i;
            if (!topics.containsKey(topicKey)) {
                // System.out.println("[INFO] Creating missing topic: " + topicKey);
                topics.put(topicKey, new Topic(topicKey)); // MODIFYING the existing `topics`, not reassigning
            }
        }
    }

    private void assignValidatorsToTopics() {
        List<Node> allNodes = new ArrayList<>(networkNodes.values());
        int topicIndex = 0, idx = 0;

        for (Node node : allNodes) {
            if (idx >= NUMBER_OF_VALIDATOR_NODES)
                break;
            if (node == blockProposerNode)
                continue; // Skip block proposer

            // System.out.println("[DEBUG] Assigning node ID: " + node.getID());

            GossipSubProtocol nodeGossip = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
            topicIndex = (idx / 128) + 1; // Assign nodes to topics in groups of 128

            Topic topic = topics.get("Topic-" + topicIndex);
            nodeGossip.subscribeTopic(topic);
            nodeGossip.localMesh.put(topic.topicID, new HashSet<>());
            topic.addMember(node);

            idx++;
        }
    }
}
