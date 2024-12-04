package peersim.gossipsub;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.dynamics.WireGraph;
import peersim.dynamics.WireWS;
import peersim.graph.BitMatrixGraph;
import peersim.graph.ConstUndirGraph;
import peersim.graph.Graph;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopicBasedMesh {
    private static final String PAR_PROT = "protocol";
    private int gossipProtocolID;

    public TopicBasedMesh(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    public void createTopicMesh()
    {
        System.out.println("Called the topic based mesh class");
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            Topic curTopic = topicEntry.getValue();
            BitMatrixGraph g = new BitMatrixGraph(curTopic.topicMembers.size(), false);
            // System.out.println("Topic ID"+ topicEntry.getValue().topicID);

             System.out.println("Topic name :"+curTopic.topicID+" size "+curTopic.topicMembers.size());

            Map<Long, Integer> nodeIdToIndex = new HashMap<>(); // A map to store the nodeID to the index position in the topic member list
            Set<BigInteger> topicMemberSet = new HashSet<>(); // A hashset containing all the members of a given topic. This is created because it is used in gossipsub protocol

            int index = 0;
            for (Node nd : curTopic.topicMembers) 
            {
                nodeIdToIndex.put(nd.getID(), index);
                GossipSubProtocol curN = (GossipSubProtocol) (nd.getProtocol(gossipProtocolID)); // Get the protocol instance of the node
                topicMemberSet.add(curN.nodeId);
                index++;
            }
            // System.out.println("topic size: "+curTopic.topicMembers.size());
//            int topicCounter =1;
            for (Node n : curTopic.topicMembers) // Looping over all the nodes in a given topic
            {

                GossipSubProtocol curNode = (GossipSubProtocol) (n.getProtocol(gossipProtocolID)); // Get the protocol instance of the node
                // System.out.println("topic size: "+curNode.localMesh.get(curTopic.topicID));
                curNode.setTopicMembersList(curTopic.topicID, topicMemberSet); //Updating/setting the current node with the list of members in the topic 

                if (curTopic.topicMembers.size() <= 1) //Meaning there in no need to form the topic network as there is one or no member
                {
                    break;
                }
//                 int cout = 1;
//                System.out.println("Curr Topic Counter : "+topicCounter++);
                while (curNode.localMesh.get(curTopic.topicID).size() < curNode.degree) 
                {
                    if(curNode.localMesh.get(curTopic.topicID).size()>=curTopic.topicMembers.size()-1)
                    {
                        break;
                    }

                    int randomIndex = (int) (Math.random() * topicEntry.getValue().topicMembers.size());
                    Node newPeer = topicEntry.getValue().topicMembers.get(randomIndex);
                    GossipSubProtocol peerNode = (GossipSubProtocol) (newPeer.getProtocol(gossipProtocolID));

                    if ((peerNode.nodeId != curNode.nodeId) && !(curNode.localMesh.get(curTopic.topicID).contains(peerNode.nodeId))) // Checking the peerNodeID is nt same the currentNodeId and its not already present in the topic list
                    {
                        curNode.localMesh.get(curTopic.topicID).add(peerNode.nodeId); // Adding the peer to the local mesh for the given topic
                        peerNode.localMesh.get(curTopic.topicID).add(curNode.nodeId); // Adding the peer to the local mesh for the given topic

                        Integer sourceIndex = nodeIdToIndex.get(n.getID());  // Index mapping for the node in the member list of the given topic
                        Integer targetIndex = nodeIdToIndex.get(newPeer.getID()); // Index mapping for the node in the member list of the given topic

                        g.setEdge(sourceIndex, targetIndex); // Adding the edges between two peers which is bidirectional
                        g.setEdge(targetIndex, sourceIndex);
                        // System.out.println(curNode.nodeId + " IS A PEER OF : " + peerNode.nodeId);
                        // System.out.println("Counter : " + cout);
                        // System.out.println("Soucre Index " + sourceIndex + " targetIndex " + targetIndex);
                        // cout++;
                    }
                }

                // System.out.println("Topic is: " + curTopic.topicID);
                // System.out.println("Topic member size is " + curTopic.topicMembers.size());
                // printGraph(g);

                // System.out.println("=======end=======");
                // break;


            }
            // break;
        }
    }


    public void printGraph(BitMatrixGraph graph)  // Function to print the graph for the local mesh topology
    {
        int size = graph.size(); 
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) 
            {
                if (graph.isEdge(i, j)) {
                    System.out.print("1 ");
                } else {
                    System.out.print("0 ");
                }
            }
            System.out.println();
        }
    }
}
