package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.graph.BitMatrixGraph;

import java.math.BigInteger;
import java.util.*;

public class TopicBasedMesh {
    private static final String PAR_PROT = "protocol";
    private int gossipProtocolID;

    public TopicBasedMesh(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    public void createTopicMesh() {
        System.out.println("Called the topic based mesh class");
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            Topic curTopic = topicEntry.getValue();
            BitMatrixGraph g = new BitMatrixGraph(curTopic.topicMembers.size(), false);

            Map<Long, Integer> nodeIdToIndex = new HashMap<>(); // A map to store the nodeID to the index position in
                                                                // the topic member list
            Set<BigInteger> topicMemberSet = new HashSet<>(); // A hashset containing all the members of a given topic.
                                                              // This is created because it is used in gossipsub
                                                              // protocol

            int index = 0;
            for (Node nd : curTopic.topicMembers) {
                nodeIdToIndex.put(nd.getID(), index);
                GossipSubProtocol curN = (GossipSubProtocol) (nd.getProtocol(gossipProtocolID)); // Get the protocol
                                                                                                 // instance of the node
                topicMemberSet.add(curN.nodeId);
                index++;
            }

            ArrayList<Node> topicNodes1 = curTopic.topicMembers;

            for (Node n : curTopic.topicMembers) // Looping over all the nodes in a given topic
            {
                GossipSubProtocol curNode = (GossipSubProtocol) (n.getProtocol(gossipProtocolID)); // Get the protocol
                                                                                                   // instance of the
                                                                                                   // node

                curNode.setTopicMembersList(curTopic.topicID, topicMemberSet); // Updating/setting the current node with
                                                                               // the list of members in the topic

                if (curTopic.topicMembers.size() <= 1) // Meaning there in no need to form the topic network as there is
                                                       // one or no member
                {
                    break;
                }
                List<Integer> indices = new ArrayList<>();
                for (int i = 0; i < curTopic.topicMembers.size(); i++) {
                    indices.add(i);
                }
                Collections.shuffle(indices);

                for (int idx : indices) {
                    if (curNode.localMesh.get(curTopic.topicID).size() >= curNode.degree) {
                        break;
                    }
                    Node newPeer = topicNodes1.get(idx);
                    GossipSubProtocol peerNode = (GossipSubProtocol) (newPeer.getProtocol(gossipProtocolID));

                    if (peerNode.localMesh.get(curTopic.topicID).size() >= peerNode.degree) {
                        continue;
                    }

                    if (!(curNode.nodeId.equals(peerNode.nodeId))
                            && !(curNode.localMesh.get(curTopic.topicID).contains(peerNode.nodeId))) {
                        curNode.localMesh.get(curTopic.topicID).add(peerNode.nodeId);
                        peerNode.localMesh.get(curTopic.topicID).add(curNode.nodeId);
                    }
                }
            }
        }
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            Topic curTopic = topicEntry.getValue();

            ArrayList<Node> topicNodes1 = curTopic.topicMembers;

            for (Node n : curTopic.topicMembers) // Looping over all the nodes in a given topic
            {
                GossipSubProtocol curNode = (GossipSubProtocol) (n.getProtocol(gossipProtocolID)); // Get the protocol
                                                                                                   // instance of the
                                                                                                   // node

                if (curTopic.topicMembers.size() <= 1) // Meaning there in no need to form the topic network as there is
                                                       // one or no member
                {
                    break;
                }
                List<Integer> indices = new ArrayList<>();
                for (int i = 0; i < curTopic.topicMembers.size(); i++) {
                    indices.add(i);
                }
                Collections.shuffle(indices);

                for (int idx : indices) {
                    if (curNode.gossipMesh.get(curTopic.topicID).size() >= curNode.degree) {
                        break;
                    }
                    Node newPeer = topicNodes1.get(idx);
                    GossipSubProtocol peerNode = (GossipSubProtocol) (newPeer.getProtocol(gossipProtocolID));

                    if (peerNode.gossipMesh.get(curTopic.topicID).size() >= peerNode.degree) {
                        continue;
                    }

                    if (!(curNode.nodeId.equals(peerNode.nodeId))
                            && !(curNode.localMesh.get(curTopic.topicID).contains(peerNode.nodeId))) {
                        curNode.gossipMesh.get(curTopic.topicID).add(peerNode.nodeId);
                        peerNode.gossipMesh.get(curTopic.topicID).add(curNode.nodeId);
                    }
                }
            }
        }

    }
}
