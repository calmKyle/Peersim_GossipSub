package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

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

            // Get the size of the topic members
            int topicSize = curTopic.topicMembers.size();

            // Collect all member IDs into a single string
            String membersString = curTopic.topicMembers.stream()
                    .map(node -> ((GossipSubProtocol) node.getProtocol(gossipProtocolID)).nodeId.toString())
                    .collect(Collectors.joining(", "));

            // Print in the requested format
            System.out.println("[TOPICS-" + curTopic.topicID + "] [SIZE=" + topicSize + "] = " + "[" + membersString + "]");

            Set<BigInteger> topicMemberSet = new HashSet<>();
            List<Node> topicNodes = new ArrayList<>(curTopic.topicMembers);
            Collections.shuffle(topicNodes, CommonState.r); // Randomize peer selection

            // Assign members to the topic
            for (Node node : curTopic.topicMembers) {
                GossipSubProtocol curNode = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
                topicMemberSet.add(curNode.nodeId);
                curNode.setTopicMembersList(curTopic.topicID, topicMemberSet);

                curNode.meshPeersByTopic.putIfAbsent(curTopic.topicID, new HashSet<>());
                curNode.gossipMesh.putIfAbsent(curTopic.topicID, new HashSet<>());
            }

            // Form initial mesh connections
            for (Node node : topicNodes) {
                GossipSubProtocol curNode = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
                connectPeers(curNode, topicNodes, curTopic.topicID);
            }
//            System.out.println("[TOPICS-" + curTopic.topicID + "] = " + "[" + curNode.meshPeersByTopic +"]");
        }
    }

    /**
     * Connects peers within a topic based on GossipSub mesh constraints.
     */
    private void connectPeers(GossipSubProtocol node, List<Node> topicNodes, String topicID) {
        List<Node> shuffledPeers = new ArrayList<>(topicNodes);
        Collections.shuffle(shuffledPeers,CommonState.r); // Ensure random connections

        for (Node peerNode : shuffledPeers) {
            if (node.meshPeersByTopic.get(topicID).size() >= node.D) {
                break; // Stop if we've reached the desired number of peers
            }

            GossipSubProtocol peerProtocol = (GossipSubProtocol) peerNode.getProtocol(gossipProtocolID);
            if (peerProtocol.meshPeersByTopic.get(topicID).size() >= peerProtocol.D) {
                continue; // Skip if the peer has reached its limit
            }

            if (!node.nodeId.equals(peerProtocol.nodeId) &&
                    !node.meshPeersByTopic.get(topicID).contains(peerProtocol.nodeId)) {
                // Establish bi-directional connection
                node.meshPeersByTopic.get(topicID).add(peerProtocol.nodeId);
                peerProtocol.meshPeersByTopic.get(topicID).add(node.nodeId);
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
            if (!peerNode.meshPeersByTopic.get(topicID).contains(node.nodeId)) {
                // Establish bi-directional connection
                peerNode.meshPeersByTopic.get(topicID).add(node.nodeId);
                node.meshPeersByTopic.get(topicID).add(peerNode.nodeId);
                count--;
            }
        }
    }

    /**
     * Removes excess peers when a node's mesh size exceeds its degree.
     */
    public void removeExcessPeers(GossipSubProtocol node, String topicID, int count) {
        Iterator<BigInteger> iterator = node.meshPeersByTopic.get(topicID).iterator();
        while (iterator.hasNext() && count > 0) {
            iterator.next();
            iterator.remove();
            count--;
        }
    }
}
