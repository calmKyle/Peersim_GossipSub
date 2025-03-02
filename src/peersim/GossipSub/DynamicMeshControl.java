package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

import java.util.Map;

public class DynamicMeshControl implements Control {
    private static final String PAR_PROT = "protocol";
    private static final boolean isDEBUG = true; // Toggle isDEBUG logging
    private final int gossipProtocolID;

    public DynamicMeshControl(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    @Override
    public boolean execute() {
        if (isDEBUG) {
            System.out.println("=== Mesh State at Cycle " + peersim.core.CommonState.getTime() + " ===");
        }

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            String topicID = topicEntry.getKey();
            StringBuilder meshInfo = new StringBuilder("Mesh for ").append(topicID).append(": ");

            // Efficient iteration over topic members
            for (Node node : topicEntry.getValue().topicMembers) {
                GossipSubProtocol gsp = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
                meshInfo.append("Node-").append(gsp.getNodeId()).append(" ");
            }

            if (isDEBUG) {
                System.out.println(meshInfo);
            }
        }

        return false; // Continue running periodically
    }
}
