package peersim.GossipSub;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

/**
 * This control initializes the whole network (that was already created by
 * peersim) assigning a unique NodeId, randomly generated,
 * to every node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class CustomDistribution implements peersim.core.Control {

    private static final String PAR_PROT = "protocol";

    // Configuration parameters
    private static final int NUMBER_OF_TOPICS = Configuration.getInt("NUMBER_OF_TOPICS", 64);
    private static final int NUMBER_OF_VALIDATOR_NODES = Configuration.getInt("NUMBER_OF_VALIDATORS", 1024);
    private static final int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC");

    private final int gossipProtocolID;
    private final UniformRandomGenerator urg;
    private static final Random random = new Random();

    public static Node blockProposerNode;

    // Custody node mappings
    public static final Map<Integer, ArrayList<BigInteger>> rowCustodyNodes = new HashMap<>();
    public static final Map<Integer, ArrayList<BigInteger>> columnCustodyNodes = new HashMap<>();

    public final String prefix;

    // Network-wide data structures
    public static final Map<BigInteger, Node> networkNodes = new LinkedHashMap<>(); // <NodeID, Node> map
    public static final Map<String, Topic> topics = new LinkedHashMap<>(NUMBER_OF_TOPICS); // <TopicID, Topic> map

    public CustomDistribution(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        this.urg = new UniformRandomGenerator(160, CommonState.r);
        this.prefix = prefix;
    }

    /**
     * Scan over the nodes in the network and assign a randomly generated NodeId in
     * the space 0..2^BITS, where BITS is a parameter
     * from the kademlia protocol (usually 160)
     *
     * @return boolean always false
     */
    public boolean execute() {
        for (int i = 0; i < Network.size(); ++i) {
            final BigInteger tmp = urg.generate(); // Generate a random BigInteger ID for the node
            Node n = Network.get(i);

            // Set node ID in GossipSubProtocol
            ((GossipSubProtocol) n.getProtocol(gossipProtocolID)).setNodeId(tmp);

            // Add node to network map
            networkNodes.put(tmp, n);

            // Set the first node as the block proposer
            if (i == 0) {
                blockProposerNode = n;
            }
        }

        try {
            initialiseTopics();
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Error initializing topics: " + e.getMessage());
        }

        return false;
    }

    /**
     * Returns the network nodes map.
     */
    public Map<BigInteger, Node> getNetworkNodes() {
        return networkNodes;
    }

    private void initialiseTopics() throws NoSuchAlgorithmException {
        System.out.println("Initializing topics for the block proposer in CustomDistribution...");

        // Get block proposer protocol instance
        GossipSubProtocol gossipBlockProposer = (GossipSubProtocol) Network.get(0).getProtocol(gossipProtocolID);

        // Initialize topics
        for (int i = 1; i <= NUMBER_OF_TOPICS; i++) {
            topics.put("Topic-" + i, new Topic("Topic-" + i));
        }

        // List of all nodes in the network (excluding block proposer)
        List<Node> allNodes = new ArrayList<>(networkNodes.values());
        allNodes.remove(blockProposerNode);

        int rowNumber = 0, columnNumber = 0, topicNumber = 0, cnt = 0, nodeCounter = 0, idx = 0;

        rowCustodyNodes.put(0, new ArrayList<>());
        columnCustodyNodes.put(0, new ArrayList<>());

        for (Node node : allNodes) {
            if (idx >= NUMBER_OF_VALIDATOR_NODES) {
                break; // Only process validator nodes
            }

            GossipSubProtocol gossipNode = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
            BigInteger nodeId = gossipNode.getNodeId();

            // Increase topic number after every 128 nodes
            if (idx % 128 == 0) {
                cnt = 0;
                nodeCounter = 0;
                topicNumber++;
            }

            // Assign custody
            if (cnt < (Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2)) {
                assignCustodyRow(gossipNode, rowNumber);
                nodeCounter++;
                if (nodeCounter % 8 == 0) {
                    rowNumber++;
                    cnt++;
                    rowCustodyNodes.put(rowNumber, new ArrayList<>());
                }
            } else {
                assignCustodyColumn(gossipNode, columnNumber);
                nodeCounter++;
                if (nodeCounter % 8 == 0) {
                    columnNumber++;
                    cnt++;
                    columnCustodyNodes.put(columnNumber, new ArrayList<>());
                }
            }

            // Subscribe node to topic
            Topic topic = topics.get("Topic-" + topicNumber);
            gossipNode.subscribeTopic(topic);
            gossipNode.localMesh.put(topic.topicID, new HashSet<>());
            gossipNode.gossipMesh.put(topic.topicID, new HashSet<>());
            topic.addMember(node);

            idx++;
        }

        System.out.println("Initial topic setup completed successfully!");
        new TopicBasedMesh(this.prefix).createTopicMesh(); // Set the mesh in each topic
    }

    /**
     * Assigns custody of a row to a node.
     */
    private void assignCustodyRow(GossipSubProtocol gossipNode, int rowNumber) {
        gossipNode.custody1 = "row" + rowNumber;
        rowCustodyNodes.computeIfAbsent(rowNumber, k -> new ArrayList<>()).add(gossipNode.nodeId);

        int randomRow = generateDifferentRandom(rowNumber);
        gossipNode.custody2 = "row" + randomRow;
        rowCustodyNodes.computeIfAbsent(randomRow, k -> new ArrayList<>()).add(gossipNode.nodeId);
    }

    /**
     * Assigns custody of a column to a node.
     */
    private void assignCustodyColumn(GossipSubProtocol gossipNode, int columnNumber) {
        gossipNode.custody1 = "column" + columnNumber;
        columnCustodyNodes.computeIfAbsent(columnNumber, k -> new ArrayList<>()).add(gossipNode.nodeId);

        int randomCol = generateDifferentRandom(columnNumber);
        gossipNode.custody2 = "column" + randomCol;
        columnCustodyNodes.computeIfAbsent(randomCol, k -> new ArrayList<>()).add(gossipNode.nodeId);
    }

    /**
     * Generates a random index different from the provided index.
     */
    private int generateDifferentRandom(int currentIndex) {
        int newIndex;
        do {
            newIndex = 8 * (currentIndex / 8) + (int) (Math.random() * 8);
        } while (newIndex == currentIndex);
        return newIndex;
    }

}

// Explanation
// Each topic will contain 8 rows and 8 cols and will have 16 nodes.
// Each node will hold either a row or col.
// So rows 0 to 7 and col 0 to 7 will be held by topic 1 and so on

// Bug
// Currently each topic doesn't have 8 cols and 8 rows(may have more or less) to
// the hash function in RowColumnDistributor class.