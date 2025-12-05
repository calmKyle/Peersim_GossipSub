package peersim.GossipSub;
//package peersim.gossipsub;
//import peersim.config.Configuration;
//import peersim.core.CommonState;
//import peersim.core.Control;
//import peersim.core.Network;
//import peersim.core.Node;
//import peersim.edsim.EDSimulator;
//
//import java.math.BigInteger;
//import java.util.Map;
//
//public class BandwidthConroller implements Control
//{
//    // ______________________________________________________________________________________________
//    /**
//     * MSPastry Protocol to act
//     */
//    private final static String PAR_PROT = "protocol";
//
//    /**
//     * MSPastry Protocol ID to act
//     */
//    private final int pid;
//
//    // ______________________________________________________________________________________________
//    public BandwidthConroller(String prefix) {
//        pid = Configuration.getPid(prefix + "." + PAR_PROT);
//
//    }
//
//    private Message resetBandwidthMessage(Node n)
//    {
//        Message m = new Message(8,"reset bandwidth",false,-1);
//        m.timestamp = CommonState.getTime();
//
//        m.dest = ((GossipSubProtocol) (n.getProtocol(pid))).nodeId;
//        return m;
//    }
//
//    public boolean execute()
//    {
////        System.out.println("Bandwidth controller executed at time: " + CommonState.getTime());
//        for (Node nd : CustomDistribution.networkNodes.values()) // Looping over all the topics
//        {
////            if(nd==CustomDistribution.blockProposerNode)
////            {
////                continue;
////            }
//            EDSimulator.add(0, resetBandwidthMessage(nd), nd, pid);
//
//        }
//
//        return false;
//    }
//}
