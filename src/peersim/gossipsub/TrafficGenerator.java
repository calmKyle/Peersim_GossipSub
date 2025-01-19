package peersim.gossipsub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

import java.math.BigInteger;
import java.util.Map;

/**
 * This control generates random search traffic from nodes to random destination
 * node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGenerator implements Control {

    // ______________________________________________________________________________________________
    /**
     * MSPastry Protocol to act
     */
    private final static String PAR_PROT = "protocol";

    /**
     * MSPastry Protocol ID to act
     */
    private final int pid;

    // ______________________________________________________________________________________________
    public TrafficGenerator(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROT);

    }

    // ______________________________________________________________________________________________
    /**
     * generates a random find node message, by selecting randomly the destination.
     *
     * @return Message
     */
    private Message startBlockProducerMessage() 
    {
        Message m = new Message(4,"You are the block producer. Start sending the data to all validator nodes in the topic",false,-1);
        m.timestamp = CommonState.getTime();

        m.dest = ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(pid))).nodeId;
        return m;
    }

//    private Message startSamplingMessage(Node n)
//    {
//        Message m = new Message(7,"start sampling",false,-1);
//        m.timestamp = CommonState.getTime();
//
//        m.dest = ((GossipSubProtocol) (n.getProtocol(pid))).nodeId;
//        return m;
//    }

    // ______________________________________________________________________________________________
    /**
     * every call of this control generates and send a random find node message
     *
     * @return boolean
     */
    public boolean execute() 
    {

        // send message
        EDSimulator.add(0, startBlockProducerMessage(), CustomDistribution.blockProposerNode, pid);

//        for (Node nd : CustomDistribution.networkNodes.values()) // Looping over all the topics
//        {
//            if(nd==CustomDistribution.blockProposerNode)
//            {
//                continue;
//            }
//            EDSimulator.add(2, startSamplingMessage(nd), nd, pid); // What is the unit of delay-------------------
//            break;
//        }

        return false;
    }

    // ______________________________________________________________________________________________

} // End of class
  // ______________________________________________________________________________________________
