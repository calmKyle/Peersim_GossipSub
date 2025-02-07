package peersim.GossipSub.PANDAS;

import java.math.BigInteger;
import peersim.core.Protocol;
import peersim.GossipSub.GossipSubProtocol;
import peersim.GossipSub.Topic;
import peersim.core.Node;

public class PeerDiscovery {
    /**
     * Retrieves the node ID of the holder of a given sample using PANDAS'
     * deterministic mapping.
     */
    public static BigInteger getNodeIdFromTopic(Topic topic, int nodeIndex, boolean isRow, int gossipSubId) {
        int offset = isRow ? 0 : 64;
        int targetIndex = nodeIndex + offset;

        if (targetIndex >= topic.topicMembers.size()) {
            return null;
        }

        Node targetNode = topic.topicMembers.get(targetIndex);
        if (targetNode != null) {
            Protocol protocol = targetNode.getProtocol(gossipSubId);
            if (protocol instanceof GossipSubProtocol) {
                return ((GossipSubProtocol) protocol).nodeId;
            }
        }
        return null;
    }
}
