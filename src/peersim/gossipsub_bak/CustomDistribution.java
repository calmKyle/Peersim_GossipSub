package peersim.gossipsub;

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
    private static final int NUMBER_OF_TOPICS = Configuration.getInt("NUMBER_OF_TOPICS", 64); // Each Topic containing total 8 rows & cols where each row/column is held by one validator node
    private static final int NUMBER_OF_VALIDATOR_NODES = Configuration.getInt("NUMBER_OF_VALIDATORS", 1024); // How many validator nodes are there per slot.
    private static final int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC");; // Number of rows and columns a topic will hold
    private int gossipProtocolID;
    private UniformRandomGenerator urg;
    private static final Random random = new Random();
    public static Node blockProposerNode;

    public String prefix;

    public static Map<BigInteger, Node> networkNodes = new LinkedHashMap<>();// <nodeID,Node> Map containing all the nodes of the network

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
        BigInteger tmp;
        for (int i = 0; i < Network.size(); ++i) {
            tmp = urg.generate(); //Createing a random BigInteger Id for the node

            Node n = Network.get(i);

            ((GossipSubProtocol) (n.getProtocol(gossipProtocolID))).setNodeId(tmp); // Setting the nodeId for the node in gossipsub protocol

            networkNodes.put(tmp, n); //Adding the node and its nodeId to the static list of nodes in the network

            if (i == 0) // Setting the first node in the network as the block producer
            {
                blockProposerNode = n;
                // System.out.println("***Block proposer ID is:***" + ((GossipSubProtocol)
                // (n.getProtocol(gossipProtocolID))).getNodeId());
            }
            // System.out.println("---Node ID is:---" + ((GossipSubProtocol)(n.getProtocol(gossipProtocolID))).getNodeId() +" "+ i+ "\n");
        }
        try {
            initialiseTopics();
        } catch (NoSuchAlgorithmException e) {
            // throw new RuntimeException(e);
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
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (Network.get(0).getProtocol(gossipProtocolID)); // Getting the gossipsub instance of the block proposer

        for (int i = 1; i <= NUMBER_OF_TOPICS; i++) // Initialising the all the topics in the network
        {
            t = new Topic("Topic-" + i);
            topics.put("Topic-" + i, t);
            // iGossipBlockProposer.subscribeTopic(t); // Subcribing the block proposer to all the topics. (NOTE: THIS LOGIC IS REMOVED AS IT IS SENDING THE MES DIRECTLY TO THE INDIVUAL NODE)
        }

        // List of all nodes in the network
        List<Node> allNodes = new ArrayList<>(networkNodes.values());

//        RowColumnDistributor r = new RowColumnDistributor(512, 512); // This class is used to initialize rows/cols to nodes. Currently, it doesn't give unique rows/cols to nodes
//        int epoch = 1;
//        int slot = 6;
        int idx = 0; // Counter to up to NUMBER_OF_VALIDATOR_NODES. Assuming the first NUMBER_OF_VALIDATOR_NODES(1024) nodes in the network at validator nodes
        int topicNumber = 0;
        for (Node node : allNodes) {
            if (node == blockProposerNode) // Skipping the node if its a block proposer
            {
                continue;
            }

            if (idx >= NUMBER_OF_VALIDATOR_NODES) // Assuming that the first 1024 nodes in the network will be the validator nodes that will receive the row/col from the block proposer
            {
                // System.out.println(("idx " + idx));
                break;
            }
            BigInteger nodeId = ((GossipSubProtocol) (node.getProtocol(gossipProtocolID))).getNodeId();

            // System.out.println("Allocation for nodeID: " + nodeId + " " + alc);
            GossipSubProtocol iGossip = (GossipSubProtocol) (node.getProtocol(gossipProtocolID)); // Get the protocol instance of the node

            if (idx % 128 == 0) // Increase the topic number adding 128 nodes to a given topic
            {
//                System.out.println("HI "+idx+" ");
                topicNumber++;
//                System.out.println(topicNumber);
            }

            iGossip.subscribeTopic(topics.get("Topic-" + topicNumber)); // Subsribing the node to the given topic
//           System.out.println("Topic Allocation for nodeID: " + nodeId + " " + topicNumber);
            iGossip.localMesh.put(topics.get("Topic-" + topicNumber).topicID, new HashSet<>());
            topics.get("Topic-" + topicNumber).addMember(node); // Adding the node to the list of members for a given topic

            idx++;
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
