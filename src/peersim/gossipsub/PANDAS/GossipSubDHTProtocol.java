package peersim.gossipsub.PANDAS;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.*;;


public class GossipSubDHTProtocol implements EDProtocol {
    private static final int DHT_LOOKUP_DELAY = 100;
    private final int kademliaProtocolId;
    private Map<BigInteger, Object> localStorage;

    public GossipSubDHTProtocol(String prefix) {
        this.kademliaProtocolId = peersim.config.Configuration.getPid(prefix + ".kademlia");
        this.localStorage = new HashMap<>();
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
        if (event instanceof SeedMessage) {
            handleSeedMessage(node, (SeedMessage) event);
        } else if (event instanceof SampleMessage) {
            handleSampleMessage(node, (SampleMessage) event);
        }
    }

    private void handleSeedMessage(Node node, SeedMessage msg) {
        System.out.println("Node " + node.getID() + " received a seed message from " + msg.getSender().getID());
        if (verifyData(msg.getData())) {
            storeDataInDHT(node, msg.getBlockID(), msg.getData());
            forwardData(node, msg);
        }
    }

    private void handleSampleMessage(Node node, SampleMessage msg) {
        if (localStorage.containsKey(msg.getBlockID())) {
            System.out.println("Serving sample request from local storage at Node " + node.getID());
        } else {
            requestDataFromDHT(node, msg.getBlockID());
        }
    }

    private boolean verifyData(Object data) {
        return data != null;
    }


    /*Need to modify kademlia a little bit  */
    private void storeDataInDHT(Node node, int blockID, Object data) {
    localStorage.put(BigInteger.valueOf(blockID), data);
    KademliaProtocol kademlia = (KademliaProtocol) node.getProtocol(kademliaProtocolId);
    if (kademlia != null) {
        kademlia.store(BigInteger.valueOf(blockID), data);
    }
}


    private void requestDataFromDHT(Node node, int blockID) {
        KademliaProtocol kademlia = (KademliaProtocol) node.getProtocol(kademliaProtocolId);
        Object data = kademlia.lookup(BigInteger.valueOf(blockID));
        if (data != null) {
            localStorage.put(BigInteger.valueOf(blockID), data);
            System.out.println("Node " + node.getID() + " retrieved block " + blockID + " from DHT.");
        } else {
            BigInteger nodeId = BigInteger.valueOf(node.getID());
            EDSimulator.add(DHT_LOOKUP_DELAY, new SampleMessage(nodeId, nodeId, blockID, 0, 0), node, kademliaProtocolId);
                }
            }
    private void forwardData(Node node, SeedMessage msg) {
        EDSimulator.add(100, new SeedMessage(node, msg.getData(), msg.getBlockID(), msg.getRows(), msg.getColumns()), node, kademliaProtocolId);
    }

    @Override
    public Object clone() {
        return new GossipSubDHTProtocol("gossipsub_dht");
    }
}
