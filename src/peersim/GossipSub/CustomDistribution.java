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
    private static final int NUMBER_OF_TOPICS = Configuration.getInt("NUMBER_OF_TOPICS", 64); // Each Topic containing
                                                                                              // total 8 rows & cols
                                                                                              // where each row/column
                                                                                              // is held by one
                                                                                              // validator node
    private static final int NUMBER_OF_VALIDATOR_NODES = Configuration.getInt("NUMBER_OF_VALIDATORS", 8192); // How many
                                                                                                             // validator
                                                                                                             // nodes
                                                                                                             // are
                                                                                                             // there
                                                                                                             // per
                                                                                                             // slot.
    private static final int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC",
            16); // Number of rows and columns a topic will hold
    private static final int NUMBER_OF_VALIDATORS_PER_TOPIC = Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC",
            128);
    private static final int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC = NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1
            ? NUMBER_OF_VALIDATORS_PER_TOPIC
            : (int) Math.ceil(
                    (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));
    // private static final int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC =
    // Configuration.getInt("NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC",8);
    // private static final int NUMBER_OF_VALIDATORS_PER_TOPIC =
    // Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC",128);

    private int gossipProtocolID;
    private UniformRandomGenerator urg;
    private static final Random random = new Random();
    public static Node blockProposerNode;
    public static HashMap<Integer, ArrayList<BigInteger>> rowCustodyNodes = new HashMap<>(); // <rowNumber<Nodes
                                                                                             // custodying this row>>
    public static HashMap<Integer, ArrayList<BigInteger>> columnCustodyNodes = new HashMap<>();
    public static Block block = new Block(Configuration.getInt("NUMBER_OF_ROWS"),
            Configuration.getInt("NUMBER_OF_COLUMNS"));

    public String prefix;

    public static Map<BigInteger, Node> networkNodes = new LinkedHashMap<>();// <nodeID,Node> Map containing all the
                                                                             // nodes of the network

    public static Map<String, Topic> topics = new LinkedHashMap<>(NUMBER_OF_TOPICS); // <TopicId,Topic>

    public CustomDistribution(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        urg = new UniformRandomGenerator(160, CommonState.r);

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
        System.out.println("NUMBER_OF_ROWSCOLS_IN_A_TOPIC " + NUMBER_OF_ROWSCOLS_IN_A_TOPIC);
        System.out.println("NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC " + NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC);
        System.out.println("NUMBER_OF_VALIDATORS_PER_TOPIC " + NUMBER_OF_VALIDATORS_PER_TOPIC);
        BigInteger tmp;
        for (int i = 0; i < Network.size(); ++i) {
            tmp = urg.generate(); // Createing a random BigInteger Id for the node

            Node n = Network.get(i);
            ((GossipSubProtocol) (n.getProtocol(gossipProtocolID))).setNodeId(tmp); // Setting the nodeId for the node
                                                                                    // in gossipsub protocol

            networkNodes.put(tmp, n); // Adding the node and its nodeId to the static list of nodes in the network

            if (i == 0) // Setting the first node in the network as the block producer
            {
                blockProposerNode = n;
            }
        }
        try {
            initialiseTopics();
        } catch (NoSuchAlgorithmException e) {

        }
        return false;
    }

    public Map<BigInteger, Node> getNetworkNodes() {
        return networkNodes;
    }

    private void initialiseTopics() throws NoSuchAlgorithmException {
        System.out.println("Initial topic setup for the block proposer in customDistribution class");
        // Initialising the topics with the name/ID
        Topic t = null;
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (Network.get(0).getProtocol(gossipProtocolID)); // Getting
                                                                                                                     // the
                                                                                                                     // gossipsub
                                                                                                                     // instance
                                                                                                                     // of
                                                                                                                     // the
                                                                                                                     // block
                                                                                                                     // proposer
        int rowNumber = 0;
        int columnNumber = 0;
        for (int i = 1; i <= NUMBER_OF_TOPICS; i++) // Initialising the all the topics in the network
        {
            t = new Topic("Topic-" + i);
            topics.put("Topic-" + i, t);
            // iGossipBlockProposer.subscribeTopic(t); // Subcribing the block proposer to
            // all the topics. (NOTE: THIS LOGIC IS REMOVED AS IT IS SENDING THE MES
            // DIRECTLY TO THE INDIVUAL NODE)
        }
        // List of all nodes in the network
        List<Node> allNodes = new ArrayList<>(networkNodes.values());

        int idx = 0; // Counter to up to NUMBER_OF_VALIDATOR_NODES. Assuming the first
                     // NUMBER_OF_VALIDATOR_NODES(1024) nodes in the network at validator nodes
        int topicNumber = 0;
        int topicNumber2 = 0;
        int cnt = 0;
        int nodeCounter = 0;
        rowCustodyNodes.put(0, new ArrayList<>());
        columnCustodyNodes.put(0, new ArrayList<>());
        int halfRowsCols = Math.max(1, NUMBER_OF_ROWSCOLS_IN_A_TOPIC / 2);
        for (int a = 0; a < allNodes.size(); a++) {
            if (allNodes.get(a) == blockProposerNode) // Skipping the node if its a block proposer
            {
                continue;
            }
            if (idx >= NUMBER_OF_VALIDATOR_NODES) // Assuming that the first 8192 nodes in the network will be the
                                                  // validator nodes that will receive the row/col from the block
                                                  // proposer
            {
                break;
            }

            if (idx % NUMBER_OF_VALIDATORS_PER_TOPIC == 0) // Increase the topic number adding 128 nodes to a given
                                                           // topic
            {
                cnt = 0;
                nodeCounter = 0;
                topicNumber++;
            }

            boolean isRow = (cnt < halfRowsCols);
            for (int b = 0; b < NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC; b++) {
                if (allNodes.get(a) == blockProposerNode) // Skipping the node if its a block proposer
                {
                    b--;
                    continue;
                }
                Node node = allNodes.get(a);
                BigInteger nodeId = ((GossipSubProtocol) (node.getProtocol(gossipProtocolID))).getNodeId();

                GossipSubProtocol iGossip = (GossipSubProtocol) (node.getProtocol(gossipProtocolID)); // Get the
                                                                                                      // protocol
                                                                                                      // instance of the
                                                                                                      // node
                iGossip.custody1 = "row" + rowNumber;
                rowCustodyNodes.get(rowNumber).add(iGossip.nodeId);
                int randomRow = rowNumber;
                while (randomRow == rowNumber) {
                    randomRow = (int) (Math.random() * ((Configuration.getInt("NUMBER_OF_ROWS"))));
                    topicNumber2 = (randomRow / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC")) + 1;
                    if (NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1) {
                        topicNumber2 = (randomRow * 2) + 1;
                    }
                }
                iGossip.custody2 = "row" + randomRow;
                if (!rowCustodyNodes.containsKey(randomRow)) {
                    rowCustodyNodes.put(randomRow, new ArrayList<>());
                }
                rowCustodyNodes.get(randomRow).add(iGossip.nodeId);

                nodeCounter++;
                a++;
                iGossip.subscribeTopic(topics.get("Topic-" + topicNumber)); // Subsribing the node to the given topic
                System.out.print("Topic Allocation for nodeID: " + nodeId + " " + topicNumber + " " + topicNumber2);
                System.out.println(" " + iGossip.custody1 + " " + iGossip.custody2);
                iGossip.localMesh.put(topics.get("Topic-" + topicNumber).topicID, new HashSet<>());
                iGossip.gossipMesh.put(topics.get("Topic-" + topicNumber).topicID, new HashSet<>());
                topics.get("Topic-" + topicNumber).addMember(node); // Adding the node to the list of members for a
                                                                    // given topic

                if (!(iGossip.isSubscribedToTopic(topics.get("Topic-" + topicNumber2)))) {
                    iGossip.subscribeTopic(topics.get("Topic-" + topicNumber2)); // Subsribing the node to the given
                                                                                 // topic
                    // System.out.print("Topic Allocation for nodeID: " + nodeId + " " +
                    // topicNumber);
                    // System.out.println(" "+iGossip.custody1+" "+iGossip.custody2);
                    iGossip.localMesh.put(topics.get("Topic-" + topicNumber2).topicID, new HashSet<>());
                    iGossip.gossipMesh.put(topics.get("Topic-" + topicNumber2).topicID, new HashSet<>());
                    topics.get("Topic-" + topicNumber2).addMember(node); // Adding the node to the list of members for a
                                                                         // given topic
                }
                idx++;
                if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                    rowNumber++;
                    cnt++;
                    rowCustodyNodes.put(rowNumber, new ArrayList<>());
                }
            }
            if (idx % NUMBER_OF_VALIDATORS_PER_TOPIC == 0) // Increase the topic number adding 128 nodes to a given
                                                           // topic
            {
                cnt = 0;
                nodeCounter = 0;
                topicNumber++;
            }
            for (int c = 0; c < NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC; c++) {
                if (allNodes.get(a) == blockProposerNode) // Skipping the node if its a block proposer
                {
                    c--;
                    continue;
                }
                Node node = allNodes.get(a);
                BigInteger nodeId = ((GossipSubProtocol) (node.getProtocol(gossipProtocolID))).getNodeId();

                // System.out.println("Allocation for nodeID: " + nodeId + " " + alc);
                GossipSubProtocol iGossip = (GossipSubProtocol) (node.getProtocol(gossipProtocolID)); // Get the
                                                                                                      // protocol
                                                                                                      // instance of the
                                                                                                      // node
                iGossip.custody1 = "column" + columnNumber;
                columnCustodyNodes.get(columnNumber).add(iGossip.nodeId);
                int randomCol = columnNumber;
                while (randomCol == columnNumber) {
                    randomCol = (int) (Math.random() * ((Configuration.getInt("NUMBER_OF_ROWS"))));
                    topicNumber2 = (randomCol / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC")) + 1;
                    if (NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1) {
                        topicNumber2 = (randomCol * 2) + 2;
                    }
                }
                iGossip.custody2 = "column" + randomCol;

                if (!columnCustodyNodes.containsKey(randomCol)) {
                    columnCustodyNodes.put(randomCol, new ArrayList<>());
                }
                columnCustodyNodes.get(randomCol).add(iGossip.nodeId);
                nodeCounter++;
                a++;
                iGossip.subscribeTopic(topics.get("Topic-" + topicNumber)); // Subsribing the node to the given topic
                System.out.print("Topic Allocation for nodeID: " + nodeId + " " + topicNumber + " " + topicNumber2);
                System.out.println(" " + iGossip.custody1 + " " + iGossip.custody2);
                iGossip.localMesh.put(topics.get("Topic-" + topicNumber).topicID, new HashSet<>());
                iGossip.gossipMesh.put(topics.get("Topic-" + topicNumber).topicID, new HashSet<>());
                topics.get("Topic-" + topicNumber).addMember(node); // Adding the node to the list of members for a
                                                                    // given topic

                if (!(iGossip.isSubscribedToTopic(topics.get("Topic-" + topicNumber2)))) {
                    iGossip.subscribeTopic(topics.get("Topic-" + topicNumber2)); // Subsribing the node to the given
                                                                                 // topic
                    // System.out.print("Topic Allocation for nodeID: " + nodeId + " " +
                    // topicNumber);
                    // System.out.println(" "+iGossip.custody1+" "+iGossip.custody2);
                    iGossip.localMesh.put(topics.get("Topic-" + topicNumber2).topicID, new HashSet<>());
                    iGossip.gossipMesh.put(topics.get("Topic-" + topicNumber2).topicID, new HashSet<>());
                    topics.get("Topic-" + topicNumber2).addMember(node); // Adding the node to the list of members for a
                                                                         // given topic
                }
                idx++;
                if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                    columnNumber++;
                    cnt++;
                    columnCustodyNodes.put(columnNumber, new ArrayList<>());
                }
            }
            a--;
        }
        System.out.println("Intial topic setup is compleeted hurry!!!!");
        TopicBasedMesh tbm = new TopicBasedMesh(this.prefix); // To set the mesh in each topic
        tbm.createTopicMesh();
    }
}

// Explanation
// Each topic will contain 8 rows and 8 cols and will have 16 nodes.
// Each node will hold either a row or col.
// So rows 0 to 7 and col 0 to 7 will be held by topic 1 and so on

// Bug
// Currently each topic doesn't have 8 cols and 8 rows(may have more or less) to
// the hash function in RowColumnDistributor class.
