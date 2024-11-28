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
    private static final int NUMBER_OF_TOPICS = 64; // Each Topic containing total 8 rows & cols where each row/column is held by one validator node
    private static final int NUMBER_OF_VALIDATOR_NODES = 1024; // How many validator nodes are there per slot.
    private int gossipProtocolID;
    private UniformRandomGenerator urg;
    private static final Random random = new Random();
    public Node blockProposerNode;

    public static Map<BigInteger, Node> networkNodes = new HashMap<>();// <nodeID,Node> Map containing all the nodes of the network

    public static Map<String, Topic> topics = new HashMap<>(NUMBER_OF_TOPICS); // <TopicId,Topic>

    public CustomDistribution(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        urg = new UniformRandomGenerator(160, CommonState.r);
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
        for (int i = 0; i < Network.size(); ++i) 
        {
            tmp = urg.generate();
            Node n = Network.get(i);
            ((GossipSubProtocol) (n.getProtocol(gossipProtocolID))).setNodeId(tmp);
            networkNodes.put(tmp, n);
            if (i == 0) {
                blockProposerNode = n;
//                System.out.println("***Block proposer ID is:***" + ((GossipSubProtocol) (n.getProtocol(gossipProtocolID))).getNodeId());
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

    public Map<BigInteger, Node> getNetworkNodes()
    {
        return networkNodes;
    }

    private void initialiseTopics() throws NoSuchAlgorithmException {

        // Initialising the topics with the name/ID
        Topic t = null;
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (Network.get(0).getProtocol(gossipProtocolID)); //Getting the gossipsub instance of the block proposer

        for (int i = 1; i <= NUMBER_OF_TOPICS; i++)
        {
            t = new Topic("Topic-" + i);
            topics.put("Topic-" + i, t);
            iGossipBlockProposer.subscribeTopic(t); // Subcribing the block proposer to all the topics
        }

        // List of all nodes in the network
        List<Node> allNodes = new ArrayList<>(networkNodes.values());

        RowColumnDistributor r = new RowColumnDistributor(512, 512);  //This class is used to initialize rows/cols to nodes. Currently, it doesn't give unique rows/cols to nodes
        int epoch = 1;
        int slot = 6;
        int idx = 1; //Counter to up to NUMBER_OF_VALIDATOR_NODES. Assuming the first NUMBER_OF_VALIDATOR_NODES(1024) nodes in the network at validator nodes
        for (Node node : allNodes) {
            if (node == blockProposerNode) // Skipping the node if its a block proposer
            {
                continue;
            }

            if (idx > NUMBER_OF_VALIDATOR_NODES) // Assuming that the first 1024 nodes in the network will be the validator nodes that will receive the row/col from the block proposer
            {
//                System.out.println(("idx " + idx));
                break;
            }
            BigInteger nodeId = ((GossipSubProtocol) (node.getProtocol(gossipProtocolID))).getNodeId();

            int alc = r.fNode(nodeId, epoch, slot); // Getting the row or col number to be allocated to the node

//            System.out.println("Allocation for nodeID: " + nodeId + " " + alc);
            GossipSubProtocol iGossip = (GossipSubProtocol) (node.getProtocol(gossipProtocolID)); // Get the protocol instance of the node

            if (alc < r.numberOfRows) { // Node will hold a row

                int topicNumber = (alc / 8) + 1; //Calculation to find which topic will this row belong to
                iGossip.subscribeTopic(topics.get("Topic-" + topicNumber));
                t.addMember(node);

            } else { // Node will hold a column
                int topicNumber = ((alc - r.numberOfRows) / 8) + 1; //Calculation to find which topic will this column belong to
                iGossip.subscribeTopic(topics.get("Topic-" + topicNumber));
                t.addMember(node);
            }
            idx++;
        }
        blockProducer();
    }

    private void blockProducer() {
        int rowNumber = 0;
        int columnNumber = 0;

        Block b = new Block(512, 512); //Creating a block

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (blockProposerNode.getProtocol(gossipProtocolID)); // Get the protocol instance of the block proposer
//        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());

        for (Map.Entry<String, Topic> topicEntry : topics.entrySet()) //Looping over all the topics
        {
            int cnt = 0;
            for (Node n : topicEntry.getValue().topicMembers)  // Looping over all the nodes in a given topic
            {

                if (cnt < 8) // Allocating first 8 nodes in the topic with rows
                {
                    int[] rowToSend = b.getRowData(rowNumber); //row to be sent

                    Message newMessage = new Message(3, rowToSend);
                    newMessage.src = iGossipBlockProposer.getNodeId();
                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipProtocolID)).getNodeId();
                    newMessage.dest = destID;

                    iGossipBlockProposer.sendMessage(newMessage, destID, gossipProtocolID); //Block proposer sending the row data to validator node
                    rowNumber++;
                    cnt++;
                }
                 else //Allocating the rest 8 nodes in the topic with columns
                 {
                     if(columnNumber==512) //added this as it causing indexoverflow error as there is a bug in row/col allocation
                     {
                         continue;
                     }
                 int[] colToSend = b.getColumnData(columnNumber);

                 Message newMessage = new Message(3,colToSend);
                 BigInteger destID = ((GossipSubProtocol)
                 n.getProtocol(gossipProtocolID)).getNodeId();

                 iGossipBlockProposer.sendMessage(newMessage,destID,gossipProtocolID);
                 columnNumber++;
                 cnt++;

                 }
            }
        }
    }
}


//Explanation
//Each topic will contain 8 rows and 8 cols and will have 16 nodes.
//Each node will hold either a row or col.
//So rows 0 to 7 and col 0 to 7 will be held by topic 1 and so on

//Bug
//Currently each topic doesn't have 8 cols and 8 rows(may have more or less) to the hash function in RowColumnDistributor class.

