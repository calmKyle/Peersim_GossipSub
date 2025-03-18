package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.Node;
import java.math.BigInteger;
import java.util.Random;

/**
 * A partially malicious variant of GossipSubProtocol that omits (drops)
 * incoming messages at a configurable rate (e.g., 0.3) and otherwise
 * forwards them normally.
 */
public class MaliciousGossipSubProtocol extends GossipSubProtocol {

    /** Fraction of messages to be dropped (e.g. 0.3 for 30% omission). */
    private double omitRate = 0.3;

    /** Random number generator for deciding which messages to drop. */
    private final Random rand = new Random();

    private boolean isDEBUG = true;

    private boolean isMalicious;

    private final int gossipSubPid; // Protocol ID for the normal gossip protocol
    private final int maliciousGossipSubPid; // Protocol ID for the malicious gossip protocol
    private final int maliciousCount; // Number of nodes to turn malicious

    public MaliciousGossipSubProtocol(String prefix) {
        super(prefix);
        this.isMalicious = true; // Mark this node as malicious.
        gossipSubPid = Configuration.getPid(prefix + ".gossipSubPid");
        maliciousGossipSubPid = Configuration.getPid(prefix + ".maliciousGossipSubPid");
        maliciousCount = Configuration.getInt(prefix + ".maliciousCount", 0);
    }

    /**
     * Clone method called by the simulator to create protocol instances.
     */
    @Override
    public Object clone() {
        MaliciousGossipSubProtocol cln = new MaliciousGossipSubProtocol(GossipSubProtocol.prefix);
        cln.omitRate = this.omitRate;
//        System.out.println("This is Omit Node");
        return cln;
    }

    // -----------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------

    /**
     * Returns true if we should drop (omit) a message, false if we should forward.
     */
    private boolean shouldOmit() {
        // Draw from uniform distribution. If random < omitRate => omit.
        return (rand.nextDouble() < omitRate);
    }

    /**
     * Helper to log that a message was dropped, if debugging is enabled.
     */
    private void logDrop(String msgType, long msgId) {
        // if (isDEBUG) {
        System.out.println("[OMISSION ATTACK] Malicious node " + nodeId
                + " DROPPED " + msgType + " message id=" + msgId);
        // }
    }

    // -----------------------------------------------------------
    // Overridden Handlers
    // -----------------------------------------------------------

    // @Override
    // public void handleIHave(Message m, int myPid) {
    // System.out.println("hanelIHAVE");
    // if (shouldOmit()) {
    // logDrop("IHAVE", m.id);
    // } else {
    // // Forward normally by calling the parent’s handler
    // super.handleIHave(m, myPid);
    // }
    // }

    @Override
    public void handleIHave(Message m, int myPid) {
        System.out.println("Entered handleIHave in MaliciousGossipSubProtocol");
        if (shouldOmit()) {
            logDrop("IHAVE", m.id);
        }else {
            super.handleIHave(m, myPid);
        }

    }

    @Override
    public void handleIWANT(Message m, int myPid) {
        if (shouldOmit()) {
            logDrop("IWANT", m.id);
        } else {
            super.handleIWANT(m, myPid);
        }
    }

    @Override
    public void handleData(Message m, int myPid) {
        if (shouldOmit()) {
            logDrop("DATA", m.id);
        } else {
            super.handleData(m, myPid);
        }
    }

    @Override
    public void handleBlockProducerData(Message m, int myPid) {
        if (shouldOmit()) {
            logDrop("BLOCK_PRODUCER_DATA", m.id);
        } else {
            super.handleBlockProducerData(m, myPid);
        }
    }

    @Override
    public void handleSampleRequest(Message m, int myPid) {
        if (shouldOmit()) {
            logDrop("SAMPLE_DATA_REQUEST", m.id);
        } else {
            super.handleSampleRequest(m, myPid);
        }
    }

    @Override
    public void handleSampleResponse(Message m, int myPid) {
        if (shouldOmit()) {
            logDrop("SAMPLE_DATA_RESPONSE", m.id);
        } else {
            super.handleSampleResponse(m, myPid);
        }
    }

    @Override
    public boolean execute() {
        if (maliciousCount <= 0) {
            return false; // No malicious nodes to create
        }

        int networkSize = Network.size();
        if (maliciousCount > networkSize) {
            throw new IllegalArgumentException("More malicious nodes configured than available in the network.");
        }

        for (int i = 0; i < maliciousCount; i++) {
            Node node = Network.get(i); // Directly get each node to modify
            node.setProtocol(maliciousGossipSubPid, new MaliciousGossipSubProtocol("maliciousGossipSub"));
        }

        return false;
    }
    

}
