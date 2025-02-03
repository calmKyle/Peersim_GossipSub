package peersim.GossipSub;
//*************************Same Class as Kademlia******
import java.util.Comparator;
import java.util.Map;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.transport.Transport;

/**
 * Initialization class that performs the bootsrap filling the k-buckets of all initial nodes.<br>
 * In particular every node is added to the routing table of every other node in the network. In the end however the various nodes
 * doesn't have the same k-buckets because when a k-bucket is full a random node in it is deleted.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class StateBuilder implements peersim.core.Control {

    private static final String PAR_PROT = "protocol";
    private static final String PAR_TRANSPORT = "transport";

    private String prefix;
    private int gossipsubid;
    private int transportid;

    public StateBuilder(String prefix) {
        this.prefix = prefix;
        gossipsubid = Configuration.getPid(this.prefix + "." + PAR_PROT);
        transportid = Configuration.getPid(this.prefix + "." + PAR_TRANSPORT);
    }

    // ______________________________________________________________________________________________
    public final GossipSubProtocol get(int i) {
        return ((GossipSubProtocol) (Network.get(i)).getProtocol(gossipsubid));
    }

    // ______________________________________________________________________________________________
    public final Transport getTr(int i) {
        return ((Transport) (Network.get(i)).getProtocol(transportid));
    }

    // ______________________________________________________________________________________________
    public static void o(Object o) {
        System.out.println(o);
    }

    // ______________________________________________________________________________________________
    public boolean execute() {

        // Sort the network by nodeId (Ascending)
//        Network.sort(new Comparator<Node>() {
//
//            public int compare(Node o1, Node o2) {
//                Node n1 = (Node) o1;
//                Node n2 = (Node) o2;
//                GossipSubProtocol p1 = (GossipSubProtocol) (n1.getProtocol(gossipsubid));
//                GossipSubProtocol p2 = (GossipSubProtocol) (n2.getProtocol(gossipsubid));
//                return Util.put0(p1.nodeId).compareTo(Util.put0(p2.nodeId));
//            }
//
//        });

        return false;
    }

} // end execute()


