package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import java.math.BigInteger;
import java.util.*;

public class TopicBasedMesh {
    private static final String PAR_PROT = "protocol";
    private final int gossipProtocolID;

    public TopicBasedMesh(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    /**
     * Creates a structured GossipSub mesh for each topic.
     */
    public void createTopicMesh() {
        System.out.println("Initializing GossipSub mesh for topics...");

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            Topic curTopic = topicEntry.getValue();
            if (curTopic.topicMembers.size() <= 1) {
                continue; // No need to form a network with only one or no member.
            }

            Set<BigInteger> topicMemberSet = new HashSet<>();
            List<Node> topicNodes = new ArrayList<>(curTopic.topicMembers);
            Collections.shuffle(topicNodes, CommonState.r); // Randomize peer selection

            // Assign members to the topic
            for (Node node : curTopic.topicMembers) {
                GossipSubProtocol curNode = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
                topicMemberSet.add(curNode.nodeId);
                curNode.setTopicMembersList(curTopic.topicID, topicMemberSet);

                curNode.localMesh.putIfAbsent(curTopic.topicID, new HashSet<>());
                curNode.gossipMesh.putIfAbsent(curTopic.topicID, new HashSet<>());
            }

            // Form initial mesh connections
            for (Node node : topicNodes) {
                GossipSubProtocol curNode = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
                connectPeers(curNode, topicNodes, curTopic.topicID);
            }
        }
    }

    /**
     * Connects peers within a topic based on GossipSub mesh constraints.
     */
    private void connectPeers(GossipSubProtocol node, List<Node> topicNodes, String topicID) {
        List<Node> shuffledPeers = new ArrayList<>(topicNodes);
        Collections.shuffle(shuffledPeers,CommonState.r); // Ensure random connections

        for (Node peerNode : shuffledPeers) {
            if (node.localMesh.get(topicID).size() >= node.degree) {
                break; // Stop if we've reached the desired number of peers
            }

            GossipSubProtocol peerProtocol = (GossipSubProtocol) peerNode.getProtocol(gossipProtocolID);
            if (peerProtocol.localMesh.get(topicID).size() >= peerProtocol.degree) {
                continue; // Skip if the peer has reached its limit
            }

            if (!node.nodeId.equals(peerProtocol.nodeId) &&
                    !node.localMesh.get(topicID).contains(peerProtocol.nodeId)) {
                // Establish bi-directional connection
                node.localMesh.get(topicID).add(peerProtocol.nodeId);
                peerProtocol.localMesh.get(topicID).add(node.nodeId);
            }
        }
    }

    /**
     * Adds more peers to maintain the required mesh size.
     */
    public void addMorePeers(GossipSubProtocol node, String topicID, int count) {
        Topic topic = CustomDistribution.topics.get(topicID);
        List<Node> potentialPeers = new ArrayList<>(topic.topicMembers);
        Collections.shuffle(potentialPeers);

        for (Node newPeer : potentialPeers) {
            if (count <= 0)
                break;

            GossipSubProtocol peerNode = (GossipSubProtocol) newPeer.getProtocol(gossipProtocolID);
            if (!peerNode.localMesh.get(topicID).contains(node.nodeId)) {
                // Establish bi-directional connection
                peerNode.localMesh.get(topicID).add(node.nodeId);
                node.localMesh.get(topicID).add(peerNode.nodeId);
                count--;
            }
        }
    }

    /**
     * Removes excess peers when a node's mesh size exceeds its degree.
     */
    public void removeExcessPeers(GossipSubProtocol node, String topicID, int count) {
        Iterator<BigInteger> iterator = node.localMesh.get(topicID).iterator();
        while (iterator.hasNext() && count > 0) {
            iterator.next();
            iterator.remove();
            count--;
        }
    }
}
