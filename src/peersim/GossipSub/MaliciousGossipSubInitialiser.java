// package peersim.GossipSub;

// import peersim.config.Configuration;
// import peersim.core.Control;
// import peersim.core.Network;
// import peersim.core.Node;

// /**
//  * This Control picks a certain number of nodes and converts their gossip
//  * protocol into a malicious version, so they drop messages.
//  */
// public class MaliciousGossipSubInitialiser implements Control {

//     // ------------------------------------------------------------------------
//     // Configuration parameters
//     // ------------------------------------------------------------------------

//     /**
//      * The protocol ID we want to replace (or set) with the malicious protocol.
//      * For example, "gossipSub" or "maliciousGossipSub" – depends on your config.
//      */
//     private static final String PAR_PROTOCOL = "protocol";

//     /**
//      * How many nodes to convert to malicious.
//      */
//     private static final String PAR_MALICIOUS_COUNT = "maliciousCount";

//     // ------------------------------------------------------------------------
//     // Fields
//     // ------------------------------------------------------------------------
//     private final int pid; // Protocol ID for the gossip protocol
//     private final int maliciousCount; // Number of nodes that become malicious

//     // ------------------------------------------------------------------------
//     // Constructor
//     // ------------------------------------------------------------------------
//     public MaliciousGossipSubInitialiser(String prefix) {
//         // Read config values
//         pid = Configuration.getPid(prefix + "." + PAR_PROTOCOL);
//         maliciousCount = Configuration.getInt(prefix + "." + PAR_MALICIOUS_COUNT, 0);
//     }

//     // ------------------------------------------------------------------------
//     // Control Interface
//     // ------------------------------------------------------------------------
//     @Override
//     public boolean execute() {
//         // If maliciousCount <= 0, do nothing
//         if (maliciousCount <= 0) {
//             System.out.println("[MaliciousGossipSubInitializer] No malicious nodes configured.");
//             return false;
//         }

//         // For simplicity, pick the first N nodes. If you prefer random selection,
//         // you can shuffle an index array or something similar.
//         System.out.println("[MaliciousGossipSubInitializer] Marking " + maliciousCount + " nodes as malicious...");

//         int networkSize = Network.size();
//         if (maliciousCount > networkSize) {
//             throw new IllegalArgumentException(
//                     "maliciousCount=" + maliciousCount + " is larger than total nodes=" + networkSize);
//         }

//         for (int i = 0; i < maliciousCount; i++) {
//             Node node = Network.get(i); // Just pick node i
//             // Create a new MaliciousGossipSubProtocol, copying any config you need
//             MaliciousGossipSubProtocol maliciousProt = new MaliciousGossipSubProtocol("maliciousGossipSub");
//             // You can also adjust dropProbability etc. here if desired
//             // maliciousProt.setDropProbability(0.4); // Hard-coded example

//             // Replace the existing protocol at pid with this malicious one
//             node.setProtocol(pid, maliciousProt);

//             System.out.println(" -> Node " + node.getID() + " is now malicious.");
//         }

//         return false; // 'false' means continue the simulation
//     }
// }
