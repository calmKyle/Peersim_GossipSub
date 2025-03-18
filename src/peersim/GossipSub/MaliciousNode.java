package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.Node;

import java.math.BigInteger;
import java.util.Random;

/**
 * A malicious variant of the GossipSubProtocol that behaves incorrectly
 * on purpose: e.g., dropping messages, sending fake data, or not responding.
 */
public class MaliciousNode extends GossipSubProtocol {

    private static final String PAR_TRANSPORT = "transport";

    // Example config param if you want a fraction of messages dropped/altered
    private static final double DROP_PROBABILITY = Configuration.getDouble("MALICIOUS_DROP_PROBABILITY", 0.3);

    // Example config param to decide whether to forge message content
    private static final double FORGE_PROBABILITY = Configuration.getDouble("MALICIOUS_FORGE_PROBABILITY", 0);

    private Random random;

    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

    public MaliciousNode(String prefix) {
        // Call the parent constructor
        super(prefix);
        this.random = new Random();
    }

    /**
     * Clone method for PeerSim. Ensures that new MaliciousNode instances
     * get their own random generator, etc.
     */
    @Override
    public Object clone() {
        MaliciousNode cln = new MaliciousNode(GossipSubProtocol.prefix);
        return cln;
    }

    /**
     * Overriding handleIHave to act maliciously:
     * e.g., randomly drop the IHAVE message or alter it.
     */
    @Override
    public void handleIHave(Message m, int myPid) {
        System.out.printf("HandleIHave in MaliciousGossipSubProtocol\n");
        if (shouldDrop()) {
            // Do nothing: malicious node discards this advertisement
            if (isDEBUG) {
                System.out.println("[MaliciousNode] Dropping IHAVE from " + m.src + " about msgID=" + m.id);
            }
        } else if (shouldForge()) {
            // For example, forge the row/column number or the message ID
            if (isDEBUG) {
                System.out
                        .println("[MaliciousNode] Forging IHAVE message from " + m.src + ": originally msgID=" + m.id);
            }
            m.id = m.id + 999999; // Just a silly forging example
            super.handleIHave(m, myPid);
        } else {
            // Otherwise, behave correctly (pass through to super)
            super.handleIHave(m, myPid);
        }
    }

    /**
     * Overriding handleIWANT to sabotage.
     */
    @Override
    public void handleIWANT(Message m, int myPid) {
        if (shouldDrop()) {
            // Maliciously ignore the request
            if (isDEBUG) {
                System.out.println("[MaliciousNode] Dropping IWANT from " + m.src + " for msgID=" + m.id);
            }
        } else if (shouldForge()) {
            // Send back incorrect data or a “corrupted” message
            if (isDEBUG) {
                System.out.println("[MaliciousNode] Forging response to IWANT from " + m.src + " for msgID=" + m.id);
            }
            // Possibly create a fake message with empty or bogus body
            Message fakeResponse = createMessage(
                    m.id,
                    Message.MSG_DATA,
                    this.nodeId,
                    m.src,
                    m.messageTopicID,
                    "FORGED_DATA".getBytes(), // bogus payload
                    m.isRow,
                    m.rowOrColumnNumber,
                    m.partNumber,
                    m.timestamp,
                    m.ackId);

            // Actually send the forged data
            publishMessage(fakeResponse, m.src, myPid);
        } else {
            // Otherwise follow normal logic
            super.handleIWANT(m, myPid);
        }
    }

    /**
     * Overriding handleData to sabotage the distribution step.
     */
    @Override
    public void handleData(Message m, int myPid) {
        // For demonstration, let's drop or pass along partial data
        if (shouldDrop()) {
            if (isDEBUG) {
                System.out.println("[MaliciousNode] Dropping DATA msgID=" + m.id + " from " + m.src);
            }
            // Do nothing
        } else if (shouldForge()) {
            if (isDEBUG) {
                System.out.println("[MaliciousNode] Forging DATA msgID=" + m.id + " from " + m.src);
            }
            // Example forging: we swap the isRow flag
            m.isRow = !m.isRow;
            super.handleData(m, myPid);
        } else {
            // Normal path
            super.handleData(m, myPid);
        }
    }

    /** Decide if we will drop a message. */
    private boolean shouldDrop() {
        return random.nextDouble() < DROP_PROBABILITY;
    }

    /** Decide if we will forge or corrupt a message. */
    private boolean shouldForge() {
        return random.nextDouble() < FORGE_PROBABILITY;
    }
}
