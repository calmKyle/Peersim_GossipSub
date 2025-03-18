package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

/**
 * This Control picks a certain number of nodes and attempts to convert their gossip
 * protocol into a malicious version, so they drop messages.
 */
public class MaliciousGossipSubInitialiser implements Control {

    // Configuration parameters
    private static final String PAR_PROTOCOL = "protocol";
    private static final String PAR_MALICIOUS_COUNT = "maliciousCount";

    // Fields
    private final int pid; // Protocol ID for the gossip protocol
    private final int maliciousCount; // Number of nodes that become malicious

    // Constructor
    public MaliciousGossipSubInitialiser(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROTOCOL);
        maliciousCount = Configuration.getInt(prefix + "." + PAR_MALICIOUS_COUNT, 0);
    }

    // Control Interface
    @Override
    public boolean execute() {
        if (maliciousCount <= 0) {
            System.out.println("[MaliciousGossipSubInitializer] No malicious nodes configured.");
            return false;
        }

        System.out.println("[MaliciousGossipSubInitializer] Marking " + maliciousCount + " nodes as malicious...");

        int networkSize = Network.size();
        if (maliciousCount > networkSize) {
            throw new IllegalArgumentException(
                    "maliciousCount=" + maliciousCount + " is larger than total nodes=" + networkSize);
        }

        for (int i = 0; i < maliciousCount; i++) {
            Node node = Network.get(i); // Picking node i

            // Assuming MaliciousGossipSubProtocol can be initialized like this and supports it:
            MaliciousGossipSubProtocol maliciousProt = new MaliciousGossipSubProtocol("maliciousGossipSub");
            // Check if node can accept a new protocol at pid
            if (node.getProtocol(pid) instanceof GossipSubProtocol) {
                node.setProtocol(pid, maliciousProt); // Setting the protocol
                System.out.println(" -> Node " + node.getID() + " is now malicious.");
            } else {
                System.out.println(" -> Failed to set malicious protocol on Node " + node.getID());
            }
        }

        return false; // Continue the simulation
    }
}
