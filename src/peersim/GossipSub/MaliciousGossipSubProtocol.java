package peersim.GossipSub;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.config.Configuration;
import peersim.transport.UnreliableTransport;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

public class MaliciousGossipSubProtocol extends GossipSubProtocol {
//    private static final String PAR_TRANSPORT = "transport";

    private static final double MESSAGE_DROP_PROBABILITY = Configuration.getDouble("MESSAGE_DROP_PROBABILITY", 0.01);
    private static final boolean ENABLE_SPAMMING = true;

    // New malicious flags
    private static final boolean ENABLE_FALSE_IHAVE = false;
    private static final boolean IGNORE_IWANT_REQUESTS = false;
    private static final boolean SEND_INVALID_DATA = false;

    private static final int SPAM_INTERVAL = 1000; // spam interval in milliseconds
    private boolean isDEBUG = true;

    public MaliciousGossipSubProtocol(String prefix) {
        super(prefix);
//        this.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
    }


    @Override
    public Object clone() {
        MaliciousGossipSubProtocol cloned = new MaliciousGossipSubProtocol(GossipSubProtocol.prefix);
        return cloned;
    }

    @Override
    public void publishMessage(Message m, BigInteger destId, int myPid) {
        // Introduce message dropping
        if (CommonState.r.nextDouble() < MESSAGE_DROP_PROBABILITY) {
            if (isDEBUG) {
                System.out.println("[MALICIOUS] Dropping message id: " + m.id + " MSG_TYPE = " + m.getType() + " at node: " + nodeId);
            }
            return; // Drop message intentionally
        }
        super.publishMessage(m, destId, myPid);
    }

    @Override
    public void handleGraft(Message m, int myPid) {
        // Malicious node ignores GRAFT requests to disrupt mesh
        if (isDEBUG) {
            System.out.println("[MALICIOUS] Ignoring GRAFT request from node: " + m.src + " at node: " + nodeId);
        }
        // Optionally send fake PRUNE response
        Message prune = createMessage(-1, Message.MSG_PRUNE, this.nodeId, m.src, m.messageTopicID, null, false, -1, -1, CommonState.getTime(), -1);
        publishMessage(prune, m.src, myPid);
    }

    @Override
    public void handlePrune(Message m, int myPid) {
        // Malicious node ignores PRUNE requests to stay in the mesh
        if (isDEBUG) {
            System.out.println("[MALICIOUS] Ignoring PRUNE request from node: " + m.src + " at node: " + nodeId);
        }
    }

//    @Override
//    public void handleIHave(Message m, int myPid) {
//        if (ENABLE_FALSE_IHAVE) {
//            // Advertise IHAVE messages for random non-existent message IDs
//            Message fakeIHave = createMessage(
//                    -9999, // deliberately fake ID
//                    Message.MSG_IHAVE,
//                    this.nodeId,
//                    m.dest,
//                    m.messageTopicID,
//                    null,
//                    m.isRow,
//                    m.rowOrColumnNumber,
//                    m.partNumber,
//                    CommonState.getTime(),
//                    -6
//            );
//            if (isDEBUG) {
//                System.out.println("[MALICIOUS] Sending false IHAVE advertisement from node: " + nodeId);
//            }
//            sendMessageToPeers(fakeIHave, myPid, m.messageTopicID, this.nodeId, m.src);
//            gossipMessageToTopicNodes(fakeIHave, myPid, m.messageTopicID, this.nodeId, m.src);
//        } else {
//            // Drop incoming IHAVE (don't propagate IHAVE)
//            if (isDEBUG) {
//                System.out.println("[MALICIOUS] Dropping IHAVE message at node: " + nodeId);
//            }
//        }
//    }

//    @Override
//    public void handleIWANT(Message m, int myPid) {
//        if (IGNORE_IWANT_REQUESTS) {
//            if (isDEBUG) {
//                System.out.println("[MALICIOUS] Ignoring IWANT request from node: " + m.src + " at node: " + nodeId);
//            }
//            return; // completely ignore IWANT request
//        }
//
//        if (SEND_INVALID_DATA) {
//            // Send corrupted data in response
//            Message corruptedResponse = createMessage(
//                    m.id,
//                    Message.MSG_DATA,
//                    nodeId,
//                    m.src,
//                    m.messageTopicID,
//                    "CORRUPTED_DATA",
//                    m.isRow,
//                    m.rowOrColumnNumber,
//                    m.partNumber,
//                    CommonState.getTime(),
//                    m.id
//            );
//            if (isDEBUG) {
//                System.out.println("[MALICIOUS] Sending corrupted data to node: " + m.src + " from: " + nodeId);
//            }
//            publishMessage(corruptedResponse, m.src, myPid);
//        } else {
//            super.handleIWANT(m, myPid); // fallback to normal behavior
//        }
//    }

    @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        super.processEvent(myNode, myPid, event);

        // Introduce spamming behavior
//        if (ENABLE_SPAMMING && event instanceof SimpleEvent && ((SimpleEvent) event).getType() == Message.MSG_HEARTBEAT) {
//            spamMessages(myPid);
//        }
    }

    private void spamMessages(int myPid) {
        // Generate and send spam message to all peers
        for (Topic topic : subscribedTopics) {
            Set<BigInteger> peers = localMesh.getOrDefault(topic.topicID, new HashSet<>());
            for (BigInteger peerId : peers) {
                Message spamMessage = createMessage(
                        -1, Message.MSG_DATA, this.nodeId, peerId,
                        topic.topicID, "spam", true, -1, -1,
                        CommonState.getTime(), -1
                );
                publishMessage(spamMessage, peerId, myPid);

                if (isDEBUG) {
                    System.out.println("[MALICIOUS] Spam message sent to: " + peerId + " from: " + nodeId);
                }
            }
        }
    }
}
