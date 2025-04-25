package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.GossipSub.Timeout;
import peersim.transport.UnreliableTransport;
import peersim.util.IncrementalStats;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static peersim.GossipSub.CustomDistribution.*;
import static peersim.GossipSub.GossipScoringConfig.TOPIC_PARAMS;

public class GossipSubProtocol implements Cloneable, EDProtocol {

    // private static final int MSG_HEARTBEAT = 9999;
    private static final long HEARTBEAT_PERIOD = 2000; // 1 second

    private Map<Long, Long> lastAdvertisedTime = new HashMap<>();
    private static final long ADVERTISEMENT_TTL = 5000;

    private boolean heartbeatScheduled = false;

    // Configuration Constants
    private static int MESSAGE_CACHE_SIZE = 1024;
    private static String PAR_TRANSPORT = "transport";
    private static int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
    private static int NUMBER_OF_VALIDATORS_PER_TOPIC = Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC", 128);
    private static int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC = NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1
            ? NUMBER_OF_VALIDATORS_PER_TOPIC
            : (int) Math.ceil(
                    (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));
    private static final double GOSSIP_FACTOR    = Configuration.getDouble("GOSSIP_FACTOR", 0.33);



    public static String prefix;

    // Instance Variables
    public BigInteger nodeId;
    public UnreliableTransport transport;
    public int tid;
    private int gossipSubId;
    public int minDegree = Configuration.getInt("MIN_DEGREE", 4);
    public int maxDegree = Configuration.getInt("MAX_DEGREE", 8);
    protected int degree = Configuration.getInt("DEGREE", 8);;

    public Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> localMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> gossipMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> topicNodes = new HashMap<>();

    private LinkedHashMap<Long, Message> messageCache = new LinkedHashMap<>();
    private LinkedHashMap<Long, Message> IWANTmessageCache = new LinkedHashMap<>();

    public IncrementalStats seedArrivalTimeStore = new IncrementalStats();
    public IncrementalStats seedPartArrivalTimeStore = new IncrementalStats();
    public IncrementalStats samplingRTTTimeStore = new IncrementalStats();

    public List<Long> messageArrivalTimeFromBP = new ArrayList<>();
    public List<Long> messageDelayTimeFromBP = new ArrayList<>();
    public List<Long> seedPartArrivalTimeFromPeer = new ArrayList<>();
    public List<Long> seedPartDelayTimeFromPeer = new ArrayList<>();
    public List<Long> sampleArrivalTime = new ArrayList<>();
    public List<Long> sampleDelayTime = new ArrayList<>();

    private List<Message> custodyData1 = new ArrayList<>();
    private List<Message> custodyData2 = new ArrayList<>();
    private List<Message> dataReceivedFromBP = new ArrayList<>();
    private List<Message> custody1Parts = new ArrayList<>();
    private List<Message> custody2Parts = new ArrayList<>();

    private List<Message> messageQueue = new ArrayList<>();
    private List<Long> messageTransmissionDelayQueue = new ArrayList<>();

    private TreeMap<Long, Message> sentMsg = new TreeMap<>();
    private TreeMap<Long, Message> sentSeedingPartMsg = new TreeMap<>();

    private int interfaceBandwidth = Configuration.getInt("INTERFACE_BANDWIDTH", 12_500_000);
    private int blockProducerBandwidth = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1_250_000_000);
    private int distributionStrategy = Configuration.getInt("DISTRIBUTION_STRATEGY");

    public long totalDataTransmitted = 0;
    public long totalTransmissionTime = 0;
    private long lastMessageTransmissionTime = 0;

    public int sampleRequestUnsuccessful = 0;
    public int noOfSampleRequestsSent = 0;
    public int noOfSamplesReceived = 0;
    public int noOfSeedPartsReceived = 0;
    private int randomSampleCounter = 0;
    public int duplicateIHaveMessage = 0 ;
    public int duplicateData = 0;


    public String custody1;
    public String custody2;
    private boolean samplingStarted = false;

    private int sampleAmount = Configuration.getInt("SAMPLE_AMOUNT", 75);

    protected static List<Set<BigInteger>> rowColHolders = new ArrayList<>(1024);

    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

    private Set<Long> seenMessageIDs = new HashSet<>();

    private HeartbeatManager heartbeatManager;

    /****************************************************************/
    // Normal nodes flag
    private boolean isBlockProposerNode = false;
    private boolean isValidatorNode = true;

    // Malicious nodes flags
    private boolean isMaliciousNode = false;  // Ommision

    public boolean isBlockProposerNode() {
        return isBlockProposerNode;
    }

    public void setBlockProposerNode(boolean isBlockProposerNode) {
        this.isBlockProposerNode = isBlockProposerNode;
    }

    public boolean isValidatorNode() {
        return isValidatorNode;
    }

    public void setValidatorNode(boolean validatorNode) {
        isValidatorNode = validatorNode;
    }

    public boolean isMaliciousNode() {
        return isMaliciousNode;
    }

    public void setMaliciousNode(boolean maliciousNode) {
        isMaliciousNode = maliciousNode;
    }
    /****************************************************************/


    public GossipSubProtocol(String prefix) {
        GossipSubProtocol.prefix = prefix;
        this.nodeId = null;
        this.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
        this.samplingStarted = false;

        this.heartbeatManager = new HeartbeatManager(
                this,
                this.ephemeralCache, // make sure ephemeralCache is still declared (e.g., as a LinkedHashMap<Long,
                                     // EphemeralMsgInfo>)
                this.peerScores, // likewise for peerScores
                this.isDEBUG);

        // for testing
        // this.isDEBUG = Configuration.contains("DEBUG_GOSSIPSUB")
        // && Configuration.getBoolean("DEBUG_GOSSIPSUB", false);
    }

    public Object clone() {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        cln.transport = (UnreliableTransport) Network.prototype.getProtocol(cln.tid);
        cln.gossipSubId = this.gossipSubId;
        return cln;
    }

    // Maps message ID -> ephemeral info
    public Map<Long, EphemeralMsgInfo> ephemeralCache = new LinkedHashMap<>();

    // Map from peer ID -> scoring info
    public Map<BigInteger, PeerScoreInfo> peerScores = new HashMap<>();

    // Weighted sum approach
    // public double computeScore(PeerScoreInfo psi) {
    // long now = CommonState.getTime();
    // double score = 0.0;

    // // Some example weights
    // double w_timeInMesh = 0.01;
    // double w_firstMsg = 0.3;
    // double w_invalid = -2.0;
    // double w_underDelivery = -1.0;

    // // (Optional) global "time in mesh" reward
    // double timeInMesh = (now - psi.timeInMeshStart);
    // double baseScore = w_timeInMesh * timeInMesh;
    // score += baseScore;

    // // Option: We'll just sum across each topic. If you prefer a "minimum"
    // approach,
    // // you'll do that instead. Let's do a sum for demonstration:
    // double topicSum = 0.0;
    // for (Map.Entry<String, PeerScoreInfo.TopicScores> entry :
    // psi.topicScoresMap.entrySet()) {
    // String topicId = entry.getKey();
    // PeerScoreInfo.TopicScores tScores = entry.getValue();
    // // Compute "underDelivery" as expected - delivered
    // tScores.underDelivery = Math.max(0, tScores.meshMsgExpected -
    // tScores.meshMsgDelivered);

    // double topicContribution = (w_firstMsg * tScores.firstMessageDeliveries)
    // + (w_invalid * tScores.invalidMessages)
    // + (w_underDelivery * tScores.underDelivery);

    // topicSum += topicContribution;
    // }

    // score += topicSum;

    // return score;
    // }
//
//    public double computeScore(PeerScoreInfo psi) {
//        return 0;
//    }

    public double computeScore(PeerScoreInfo psi) {
        long now = CommonState.getTime();
        double totalScore = 0.0;

        // For each topic this peer is associated with:
        for (Map.Entry<String, PeerScoreInfo.TopicScores> entry : psi.topicScoresMap.entrySet()) {
            String topicId = entry.getKey();
            PeerScoreInfo.TopicScores tScores = entry.getValue();

            // Get the scoring parameters for this topic
            GossipScoringConfig.TopicParam param = TOPIC_PARAMS.get(topicId);
            if (param == null) {
                // If no known param for this topic, skip or apply a default
                continue;
            }

            double topicScore = 0.0;

            // 1) Time in mesh
            double timeInMesh = now - psi.timeInMeshStart;
            double cappedTime = Math.min(timeInMesh, param.timeInMeshCap);
            double timeInMeshPart = param.timeInMeshWeight * cappedTime;
            topicScore += timeInMeshPart;

            // 2) First message deliveries
            // Possibly your decays happen in HeartbeatManager; here we just sum
            double firstDelivCount = tScores.firstMessageDeliveries;
            double cappedFirstDeliv = Math.min(firstDelivCount, param.firstMessageDeliveriesCap);
            double firstDelivScore = param.firstMessageDeliveriesWeight * cappedFirstDeliv;
            topicScore += firstDelivScore;

            // 3) Mesh deliveries
            double meshDelivered = tScores.meshMsgDelivered;
            double cappedMesh = Math.min(meshDelivered, param.meshMessageDeliveriesCap);
            // Optionally compare with param.meshMessageDeliveriesThreshold if you want
            double meshDelivScore = param.meshMessageDeliveriesWeight * cappedMesh;
            topicScore += meshDelivScore;

            // 4) Invalid messages
            double invalidCount = tScores.invalidMessages;
            double invalidScore = param.invalidMessageDeliveriesWeight * invalidCount;
            topicScore += invalidScore;

            // Multiply final by param.topicWeight (if desired).
            topicScore *= param.topicWeight;

            // Add to total
            totalScore += topicScore;
        }

        psi.cachedScore = totalScore; // store for quick reference
        return totalScore;
    }

    public void advertiseMessageIHAVE(Message originalMsg, int myPid) {
        Set<BigInteger> peers = localMesh.getOrDefault(originalMsg.messageTopicID, Collections.emptySet());
        if (peers.isEmpty()) return;

        int advertiseCnt = Math.max(1, (int)Math.ceil(GOSSIP_FACTOR * peers.size()));
        List<BigInteger> shuffled = new ArrayList<>(peers);
        Collections.shuffle(shuffled, CommonState.r);

        Message ihave = createMessage(originalMsg.id, Message.MSG_IHAVE, nodeId,null,
                originalMsg.messageTopicID,null,
                originalMsg.isRow,originalMsg.rowOrColumnNumber,
                originalMsg.partNumber,CommonState.getTime(),-6);

        for (int i=0;i<advertiseCnt;i++){
            BigInteger peer = shuffled.get(i);
            if (peer.equals(nodeId)) continue;
            Message copy = (Message) ihave.copy();
            copy.dest = peer;
            publishMessage(copy, peer, myPid);
        }
    }

//    public void advertiseMessageIHAVE(Message originalMsg, int myPid) {
//        if (isDEBUG) {
//            System.out.println("[DEBUG] Node " + nodeId +
//                    " advertiseMessageIHAVE for msgID=" + originalMsg.id);
//        }
//
//        Message ihave = createMessage(
//                originalMsg.id,
//                Message.MSG_IHAVE,
//                this.nodeId,
//                null,
//                originalMsg.messageTopicID,
//                null,
//                originalMsg.isRow,
//                originalMsg.rowOrColumnNumber,
//                originalMsg.partNumber,
//                CommonState.getTime(),
//                -6);
//
//        Set<BigInteger> peers = localMesh.getOrDefault(originalMsg.messageTopicID, new HashSet<>());
//        for (BigInteger peer : peers) {
//            if (peer.equals(this.nodeId))
//                continue;
//            if (isDEBUG) {
//                System.out.println("[DEBUG] -> Sending IHAVE(msgID=" + originalMsg.id +
//                        ") to peer=" + peer + " from node=" + nodeId);
//            }
//            Message copy = (Message) ihave.copy();
//            copy.dest = peer;
//            publishMessage(copy, peer, myPid);
//        }
//    }
//
//    public void removePeerFromMesh(BigInteger peerID) {
//        // remove from all topics in localMesh
//        for (String topicID : localMesh.keySet()) {
//            localMesh.get(topicID).remove(peerID);
//        }
//        // remove from scoring as well, or optionally keep as "disconnected" data
//        peerScores.remove(peerID);
//    }
//
//    public void setMeshDegree(int newDegree) {
//        if (newDegree >= minDegree && newDegree <= maxDegree) {
//            this.degree = newDegree;
//            updateMeshConnections();
//        } else {
//            if (isDEBUG) {
//                System.out.println("[DEGREE ISSUE:]Degree must be between " + minDegree + " and " + maxDegree);
//            }
//        }
//    }

    public void updateMeshConnections() {
        if (subscribedTopics.isEmpty())
            return;

        for (String topicID : subscribedTopics.stream().map(t -> t.topicID).collect(Collectors.toList())) {
            localMesh.putIfAbsent(topicID, new HashSet<>());
            Set<BigInteger> peers = localMesh.get(topicID);
            if (peers == null)
                continue;

            if (peers.size() < degree) {
                addMorePeers(topicID, degree - peers.size());
            }
            if (peers.size() > degree) {
                removeExcessPeers(topicID, peers.size() - degree);
            }
        }
    }
    private boolean inMyMesh(String topicID, BigInteger peer) {
        Set<BigInteger> mesh = localMesh.get(topicID);
        return mesh != null && mesh.contains(peer);
    }

    // Function to add new peers when the mesh is too small
    public void addMorePeers(String topicID, int needed) {
        Topic topic = CustomDistribution.topics.get(topicID);
        if (topic == null)
            return;

        List<Node> potentialPeers = new ArrayList<>(topic.topicMembers);
        Collections.shuffle(potentialPeers, CommonState.r);

        for (Node newPeer : potentialPeers) {
            // If we've already reached our local 'degree', stop adding more
            if (localMesh.get(topicID).size() >= degree) {
                break;
            }

            if (needed <= 0)
                break;

            GossipSubProtocol peerNode = (GossipSubProtocol) newPeer.getProtocol(gossipSubId);

            // If that peer already has that node, skip
            // (only if you want to reduce double-link creation)
            if (peerNode.localMesh.get(topicID).contains(this.nodeId)) {
                continue;
            }

            // We add them to OUR local mesh
            localMesh.putIfAbsent(topicID, new HashSet<>());
            localMesh.get(topicID).add(peerNode.nodeId);

            addPeerScoreIfAbsent(peerNode.nodeId);

            // Then we send them a GRAFT, so *they* can decide if they want us in their mesh
            Message graft = createMessage(
                    -1,
                    Message.MSG_GRAFT,
                    this.nodeId,
                    peerNode.nodeId,
                    topicID,
                    null,
                    false, -1, -1,
                    CommonState.getTime(),
                    -1);
            publishMessage(graft, peerNode.nodeId, gossipSubId);

            // We used up one slot
            needed--;

            // If we want to be REALLY sure we don't overshoot, we do:
            if (localMesh.get(topicID).size() >= degree) {
                break;
            }
        }
    }

    public void handleGraft(Message m, int myPid) {
        String topicID = m.messageTopicID;
        BigInteger p = m.src; // the peer that is trying to graft onto me

        // For convenience, let's store the current time
        long now = CommonState.getTime();

        // Log that got a GRAFT
        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] Node " + nodeId
                    + " received GRAFT from peer " + p
                    + " for topic=" + topicID
                    + " at time=" + now);
        }

        // Ensure we have a PeerScoreInfo for this peer
        addPeerScoreIfAbsent(p);
        PeerScoreInfo psi = peerScores.get(p);
        if (psi == null) {
            if (isDEBUG) {
                System.out.println("[DEBUG handleGraft] No PeerScoreInfo for " + p
                        + "; ignoring GRAFT.");
            }
            return;
        }

        // Compute their score and do backoff checks
        double s = computeScore(psi);
        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] Peer " + p + " has score=" + s
                    + ", time=" + now
                    + ", pruneBackoffUntil=" + psi.pruneBackoffUntil);
        }

        // If negative score or in backoff, we prune them immediately
        if (s < 0 || now < psi.pruneBackoffUntil) {
            if (isDEBUG) {
                System.out.println("[DEBUG handleGraft] Peer " + p
                        + " is being PRUNE'd (score<0 or in backoff).");
            }
            Message prune = createMessage(
                    -1,
                    Message.MSG_PRUNE,
                    this.nodeId,
                    p,
                    topicID,
                    null, false, -1, -1,
                    now,
                    -1);
            publishMessage(prune, p, myPid);
            return; // do not add them to my local mesh
        }

        // Otherwise, accept them in my local mesh for topicID
        localMesh.putIfAbsent(topicID, new HashSet<>());
        localMesh.get(topicID).add(p);

        // Just in case, ensure we track peer's score info
        addPeerScoreIfAbsent(p);

        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] Node " + nodeId
                    + " accepted peer " + p
                    + " into mesh for topic=" + topicID
                    + ". Current mesh size=" + localMesh.get(topicID).size());
        }

        // If oversubscribed, remove some peers
        if (localMesh.get(topicID).size() > maxDegree) {
            int over = localMesh.get(topicID).size() - degree;   // trim back to “degree”
            if (isDEBUG) {
                System.out.printf("[DEBUG handleGraft] mesh %s oversized (%d>%d); pruning %d peers%n",
                        topicID, localMesh.get(topicID).size(), maxDegree, over);
            }
            removeExcessPeers(topicID, over);
        }
    }

    public void handlePrune(Message m, int myPid) {
        BigInteger pruner = m.src; // the node that is pruning me
        String topicID = m.messageTopicID;
        long now = CommonState.getTime();

        //  Log that received a PRUNE
        if (isDEBUG) {
            System.out.println("[DEBUG handlePrune] Node " + nodeId
                    + " received PRUNE from " + pruner
                    + " for topic=" + topicID
                    + " at time=" + now);
        }

        //  Remove that node from local mesh (if present).
        Set<BigInteger> meshPeers = localMesh.getOrDefault(topicID, new HashSet<>());
        boolean wasPresent = meshPeers.remove(pruner);

        if (!wasPresent) {
            // Not in our mesh anyway
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] Node " + nodeId
                        + " wasn't tracking pruner=" + pruner
                        + " in localMesh for topic=" + topicID
                        + ", ignoring.");
            }
            return;
        }

        // Now check if this removal dropped our mesh below minDegree
        int sizeAfterRemoval = meshPeers.size();
        if (isDEBUG) {
            System.out.println("[DEBUG handlePrune] After removing " + pruner
                    + ", localMesh[" + topicID + "] size=" + sizeAfterRemoval
                    + " for node=" + nodeId);
        }

        if (sizeAfterRemoval < minDegree) {
            int needed = degree - sizeAfterRemoval;
            if (needed > 0) {
                if (isDEBUG) {
                    System.out.println("[DEBUG handlePrune] localMesh[" + topicID + "] too small ("
                            + sizeAfterRemoval + " < minDegree=" + minDegree
                            + "). GRAFTing " + needed + " peers...");
                }
                addMorePeers(topicID, needed);
            }
        }

        //  If for some reason end up bigger than maxDegree, removeExcessPeers
        if (sizeAfterRemoval > maxDegree) {
            int over = sizeAfterRemoval - degree;
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] localMesh[" + topicID + "] oversubscribed size="
                        + sizeAfterRemoval + " > maxDegree=" + maxDegree
                        + ". removing 'over'=" + over + " peers...");
            }
            removeExcessPeers(topicID, over);
        }

        // OPTIONAL: record that pruner is refusing me until T
        // e.g., peerRefuseUntil.put(pruner, now + someBackoff);
    }

    // e.g. in removeExcessPeers(...) or prunePeer(...)
    public void prunePeer(BigInteger peerID, String topicID) {
        long now = CommonState.getTime();

        //  Remove from local mesh
        Set<BigInteger> meshPeers = localMesh.getOrDefault(topicID, new HashSet<>());
        boolean wasPresent = meshPeers.remove(peerID);

        if (isDEBUG) {
            System.out.println("[DEBUG prunePeer] Node " + nodeId
                    + " is pruning peer " + peerID
                    + " for topic=" + topicID
                    + " at time=" + now);
            if (!wasPresent) {
                System.out.println("[DEBUG prunePeer] peer " + peerID
                        + " wasn't in localMesh[" + topicID + "] anyway; ignoring.");
            } else {
                System.out.println("[DEBUG prunePeer] localMesh[" + topicID + "] size is now "
                        + meshPeers.size() + " after removing " + peerID);
            }
        }

        if (!wasPresent) {
            // If the peer wasn't in our mesh, no need to send a PRUNE or set backoff
            return;
        }

        // Send a PRUNE message so they know we've removed them
        Message prune = createMessage(
                -1,
                Message.MSG_PRUNE,
                this.nodeId,
                peerID,
                topicID,
                null,
                false,
                -1,
                -1,
                now,
                -1);
        publishMessage(prune, peerID, gossipSubId);

        // Set backoff
        PeerScoreInfo psi = peerScores.get(peerID);
        if (psi != null) {
            long backoff = 60000; // 1 minute
            psi.pruneBackoffUntil = now + backoff;

            if (isDEBUG) {
                System.out.println("[DEBUG prunePeer] Setting pruneBackoffUntil="
                        + psi.pruneBackoffUntil
                        + " for peer " + peerID);
            }
        }
    }

    private void removeExcessPeers(String topicID, int count) {
        Set<BigInteger> peers = localMesh.get(topicID);
        if (peers == null)
            return;

        int maxAllowed = peers.size() - minDegree;
        count = Math.min(count, maxAllowed);
        if (count <= 0) return;

        // Make sure each peer has a PeerScoreInfo
        for (BigInteger p : peers) {
            addPeerScoreIfAbsent(p);
        }

        // Then do the sorting:
        List<BigInteger> sorted = new ArrayList<>(peers);
        sorted.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));

        // Prune the worst 'count' peers
        for (int i = 0; i < count; i++) {
            BigInteger toRemove = sorted.get(i);
            prunePeer(toRemove, topicID);
        }
    }


    private void addPeerScoreIfAbsent(BigInteger peerID) {
        if (!peerScores.containsKey(peerID)) {
            peerScores.put(peerID, new PeerScoreInfo(CommonState.getTime()));
        }
    }

    private int calculateMessageSize(Message message) {
        int size = 0;

        // Fixed-size fields
        size += 2 * Integer.BYTES + 2 * Long.BYTES + 1; // Includes final 'size += 1'

        // Variable-length fields
        size += getSize(message.src);
        size += getSize(message.dest);
        size += getSize(message.messageTopicID);

        // Handle different body types
        size += getBodySize(message.body);

        return size;
    }

    // Helper method to get size of a nullable String
    private int getSize(Object obj) {
        return (obj != null) ? obj.toString().getBytes().length : 0;
    }

    // Helper method to calculate size of message body
    private int getBodySize(Object body) {
        int size = 0;
        if (body instanceof byte[][]) {
            for (byte[] row : (byte[][]) body) {
                size += row.length;
            }
        } else if (body instanceof byte[][][]) {
            for (byte[][] matrix : (byte[][][]) body) {
                for (byte[] row : matrix) {
                    size += row.length;
                }
            }
        } else if (body instanceof String) {
            size += ((String) body).getBytes().length;
        }
        return size;
    }

    public void subscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            return;
        }
        subscribedTopics.add(topic);
    }

    public void unsubscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            subscribedTopics.remove(topic);
            localMesh.remove(topic.topicID);
            return;
        }
    }

    public boolean isSubscribedToTopic(Topic topic) {
        return subscribedTopics.contains(topic);
    }

    public void setTopicMembersList(String topicName, Set<BigInteger> members) {
        topicNodes.put(topicName, members);
    }

    public void publishMessage(Message m, BigInteger destId, int myPid) {

        // Malicious
        if (isMaliciousNode() && !m.src.equals(this.nodeId) ) {
            if (isDEBUG) {
                System.out.println("[MALICIOUS‑DROP] node " + nodeId +
                        " dropped fwd of msg " + m.id);
            }
            return;
        }

        int bandwidth = (this.isBlockProposerNode())
                ? blockProducerBandwidth
                : interfaceBandwidth;

        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        long latency = transport.getLatency(src, dest);

        while (!messageTransmissionDelayQueue.isEmpty()
                && CommonState.getTime() > messageTransmissionDelayQueue.get(0)) {
            messageQueue.remove(0);
            messageTransmissionDelayQueue.remove(0);
        }

        long queuingDelay = messageQueue.isEmpty()
                ? 0
                : messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1)
                        - CommonState.getTime();

        int messageSize = calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / (double) (bandwidth / 1000));
        long propagationDelay = latency;
        long totalDelay = queuingDelay + transmissionDelay + propagationDelay;

        totalDataTransmitted += messageSize;
        totalTransmissionTime += totalDelay;

        EDSimulator.add(totalDelay, m, dest, myPid);

        long scheduledTransmissionTime = CommonState.getTime() + transmissionDelay;
        messageQueue.add(m);
        messageTransmissionDelayQueue.add(scheduledTransmissionTime);
        lastMessageTransmissionTime = scheduledTransmissionTime;
    }

    public void sendMessageToTopicNodes(Message m, int myPid, String topicID,
            Map<String, Set<BigInteger>> nodesInTopic,
            BigInteger messageSender, BigInteger src) {
        Set<BigInteger> topicNodes = nodesInTopic.get(topicID);
        if (topicNodes == null)
            return;



        for (BigInteger peerId : topicNodes) {
            if (peerId.equals(messageSender) || peerId.equals(m.src)) {
                continue;
            }
            // Malicious
//            if (this.isMaliciousNode() ) {
//                if (isDEBUG) {
//                    System.out.println("[MALICIOUS] Node " + this.nodeId
//                            + " is dropping message ID=" + m.id + " instead of sendMessageToTopicNodes.");
//                }
//                return;
//            }
            Message newMessage = createMessage(
                    m.id, m.type, src, peerId, m.messageTopicID,
                    m.body, m.isRow, m.rowOrColumnNumber, m.partNumber,
                    CommonState.getTime(), (m.typeID == -6) ? m.typeID : m.id);
            publishMessage(newMessage, peerId, myPid);
        }
    }

    public void sendMessageToPeers(Message m, int myPid, String topicID,
            BigInteger avoid, BigInteger src) {
        Set<BigInteger> peers = localMesh.get(topicID);
        if (peers == null || peers.isEmpty()) {
            if (isDEBUG) {
                System.out.println("[ERROR SEND MESSAGE: ] local mesh empty for node: " + nodeId);
            }
            return;
        }
        sendMessageToTopicNodes(m, myPid, topicID, localMesh, avoid, src);
    }

    public void gossipMessageToTopicNodes(
            Message m, int myPid, String topicID,
            BigInteger avoid, BigInteger src) {
        m.body = null; // only metadata
        sendMessageToTopicNodes(m, myPid, topicID, gossipMesh, avoid, src);
    }

    public void createWholeRowOrColumnSeed(Message m, int rowOrColNum) {
        byte[][] body = m.isRow ? block.getRowData(rowOrColNum) : block.getColumnData(rowOrColNum);
        Message newSeed = createMessage(
                -1, 3, nodeId, nodeId, m.messageTopicID, body,
                m.isRow, m.rowOrColumnNumber, -1, CommonState.getTime(), -1);
        custodyData1.add(newSeed);
        samplingStarter();
    }

    public void handleReceivedPart(Message m, int myPid) {
        String s = m.isRow ? "row" : "column";
        String key = s + m.rowOrColumnNumber;
        int threshold = (NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC + 1) / 2;

        if (custody1.equals(key)) {
            processCustody(m, custody1Parts, threshold);
        } else if (custody2.equals(key)) {
            processCustody(m, custody2Parts, threshold);
        }
    }

    private void storeInEphemeralCache(Message m) {
        if (!ephemeralCache.containsKey(m.id)) {
            ephemeralCache.put(m.id, new EphemeralMsgInfo(m));
            if (isDEBUG) {
                System.out.println("[DEBUG] Node " + nodeId +
                        " storeInEphemeralCache msgID=" + m.id);
            }
        }
    }

    private void processCustody(Message m, List<Message> custodyParts, int threshold) {


        if (custodyParts.contains(m)) {
            duplicateData++;
            return;
        }


        if (custodyParts.size() < threshold) {

            long now = CommonState.getTime();
            seedPartArrivalTimeFromPeer.add(now);
            seedPartDelayTimeFromPeer.add(now - m.timestamp);
            seedPartArrivalTimeStore.add(now);

            custodyParts.add(m);

            if (custodyParts.size() == threshold) {
                createWholeRowOrColumnSeed(m, m.rowOrColumnNumber);
            }
        } else {
            // We already reached the threshold; treat any further
            // piece as a duplicate of the finished set.
            duplicateData++;
        }
    }


//    private void processCustody(Message m, List<Message> custodyParts, int threshold) {
//        if (custodyParts.size() < threshold) {
//            long currentTime = CommonState.getTime();
//            seedPartArrivalTimeFromPeer.add(currentTime);
//            seedPartDelayTimeFromPeer.add(currentTime - m.timestamp);
//            seedPartArrivalTimeStore.add(currentTime - m.timestamp);
//            custodyParts.add(m);
//
//            if (custodyParts.size() == threshold) {
//                createWholeRowOrColumnSeed(m, m.rowOrColumnNumber);
//            }
//        }
//    }

//    public void handleReceivedRowOrCol(Message m, int myPid) {
//        String s = m.isRow ? "row" : "column";
//        String key = s + m.rowOrColumnNumber;
//
//        if (custody1.equals(key)) {
//            custodyData1.add(m);
//        } else if (custody2.equals(key)) {
//            custodyData2.add(m);
//        } else {
//            return;
//        }
//
//        long currentTime = CommonState.getTime();
//        messageArrivalTimeFromBP.add(currentTime);
//        messageDelayTimeFromBP.add(currentTime - m.timestamp);
//        seedArrivalTimeStore.add(currentTime);
//    }

    public void handleReceivedRowOrCol(Message m, int myPid) {
        String key = (m.isRow ? "row" : "column") + m.rowOrColumnNumber;

        if (custody1.equals(key)) {                       // message belongs to custody‑1
            if (custodyData1.contains(m)) {               // we already have this message
                duplicateData++;                          // …so just count it
            } else {
                custodyData1.add(m);                      // first time we see it
            }

        } else if (custody2.equals(key)) {                // message belongs to custody‑2
            if (custodyData2.contains(m)) {
                duplicateData++;
            } else {
                custodyData2.add(m);
            }

        } else {                                          // not my custody – ignore
            return;
        }

        long now = CommonState.getTime();
        messageArrivalTimeFromBP.add(now);
        messageDelayTimeFromBP.add(now - m.timestamp);
        seedArrivalTimeStore.add(now);
    }


    // double check on this !
    public void samplingStarter() {
        if (samplingStarted)
            return;

        boolean shouldStart = false;

        if (distributionStrategy == 3) {
            // Start sampling if EITHER custodyData1 OR custodyData2 has 1 piece
            if (custodyData1.size() >= 1 || custodyData2.size() >= 1) {
                shouldStart = true;
            }
        } else if (distributionStrategy == 2) {
            if (custodyData1.size() >= 2) {
                shouldStart = true;
            }
        }

        if (shouldStart) {
            samplingStarted = true;
            startSampling();
        }
    }



    public void handleIHave(Message m, int myPid) {
        if (messageCache.containsKey(m.id)) {
            //handle duplicate I have
            duplicateIHaveMessage++;
            if (IWANTmessageCache.containsKey(m.id) && m.body != null) { // Late-arriving message
                processReceivedMessage(m, myPid);
            }
            return;
        }
        messageCache.put(m.id, m);

        if (m.typeID == -6) {
            handleTypeMessage(m, myPid);
        }


        messageCache.put(m.id, m);

        storeInEphemeralCache(m);

        long now = CommonState.getTime();
        if (lastAdvertisedTime.containsKey(m.id)
                && (now - lastAdvertisedTime.get(m.id) < ADVERTISEMENT_TTL)) {

            // Node already gossiped this IHAVE not too long ago => skip re-gossip
            return;
        }

        lastAdvertisedTime.put(m.id, now);


        Message responseWithMetaData = createMessage(m.id, Message.MSG_IHAVE, m.src,
                m.dest, m.messageTopicID, null,
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), -6);

        Message responseWithData = createMessage(m.id, Message.MSG_IHAVE, m.src,
                m.dest, m.messageTopicID, m.body,
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), -6);

        responseWithData.src = m.src;

        // advertiseMessageIHAVE(m, myPid);
        incrementDelivered(m.src, m.messageTopicID);
        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, this.nodeId,
                m.src);
        gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,
                this.nodeId, m.src);
        samplingStarter();
    }

    // Process a message that arrives late after sending IHAVE
    private void processReceivedMessage(Message m, int myPid) {
        if (distributionStrategy == 3) {
            handleReceivedRowOrCol(m, myPid);
        } else if (distributionStrategy == 2) {
            handleReceivedPart(m, myPid);
        }
        messageCache.put(m.id, m);
        IWANTmessageCache.remove(m.id);
        samplingStarter();
    }

    private void handleTypeMessage(Message m, int myPid) {
        String key = (m.isRow ? "row" : "column") + m.rowOrColumnNumber;

        if (distributionStrategy == 3) {
            if (!custodyData1.contains(m) && !custodyData2.contains(m)) {
                if (custody1.equals(key) || custody2.equals(key)) {
                    if (m.body == null) {
                        requestMissingData(m, myPid, custodyData1.isEmpty() ? custody1 : custody2);
                    } else {
                        handleReceivedRowOrCol(m, myPid);
                    }
                }
            }
        } else if (distributionStrategy == 2) {
            if (!custodyData1.contains(m)) {
                if (custody1.equals(key) || custody2.equals(key)) {
                    if (m.body == null) {
                        requestMissingPart(m, myPid, custody1Parts.size(), custody2Parts.size(), custody1, custody2);
                    } else {
                        handleReceivedPart(m, myPid);
                    }
                }
            }
        }
    }

    // Send IWANT message if data is missing
    private void requestMissingData(Message m, int myPid, String custody) {
        Message request = createMessage(m.id, Message.MSG_IWANT, nodeId, m.src, m.messageTopicID, "",
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), m.typeID);

        IWANTmessageCache.put(m.id, m);
        publishMessage(request, m.src, myPid);
    }

    // Send IWANT message if a part is missing
    private void requestMissingPart(Message m, int myPid, int custody1Size, int custody2Size, String custody1,
            String custody2) {
        int threshold = (NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC + 1) / 2;
        String key = (m.isRow ? "row" : "column") + m.rowOrColumnNumber;

        if (custody1.equals(key) && custody1Size < threshold) {
            sendIWantMessage(m, myPid);
        } else if (custody2.equals(key) && custody2Size < threshold) {
            sendIWantMessage(m, myPid);
        }
    }

    // Helper method to create and send IWANT messages
    private void sendIWantMessage(Message m, int myPid) {
        Message request = createMessage(m.id, Message.MSG_IWANT, nodeId, m.src, m.messageTopicID, "",
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), m.typeID);

        IWANTmessageCache.put(m.id, m);
        publishMessage(request, m.src, myPid);
    }

    public void handleIWANT(Message m, int myPid) {
        if (isDEBUG) {
            System.out.println("[DEBUG handleIWANT] Node " + nodeId
                    + " received IWANT for msgId=" + m.id
                    + " from node " + m.src);
        }

        // Try to find the requested message among your local caches/collections.
        // For example, if you store complete messages in 'messageCache', do:
        Message storedMsg = messageCache.get(m.id);

        // If we don't have it, do nothing (or log).
        // The requesting node may ask another peer.
        if (storedMsg == null || storedMsg.body == null) {
            if (isDEBUG) {
                System.out.println("[DEBUG handleIWANT] Node " + nodeId
                        + " => does NOT have msgId=" + m.id + ", ignoring request.");
            }
            return;
        }

        // We do have it. Construct a MSG_DATA response with the full body.
        // Note: we re-use m.id, plus 'nodeId' as our src, and 'm.src' as the dest.
        // The body is the actual payload from 'storedMsg'.
        Message response = createMessage(
                m.id,
                Message.MSG_DATA,
                nodeId, // from me
                m.src, // back to the requester
                storedMsg.messageTopicID,
                storedMsg.body, // the actual payload
                storedMsg.isRow,
                storedMsg.rowOrColumnNumber,
                storedMsg.partNumber,
                CommonState.getTime(), // timestamp
                m.id // ackId can track which message we're responding to
        );

        // (Optional) store it in ephemeral cache if your logic requires
        storeInEphemeralCache(response);

        // Publish/forward the full data back to the requester
        publishMessage(response, m.src, myPid);

        if (isDEBUG) {
            System.out.println("[DEBUG handleIWANT] Node " + nodeId
                    + " => Sent MSG_DATA for msgId=" + m.id
                    + " to node " + m.src);
        }
    }

    // public void handleIWANT(Message m, int myPid) {
    // if (distributionStrategy == 3) {
    // if (findAndSendResponse(m, myPid, custodyData1) ||
    // findAndSendResponse(m, myPid, custodyData2) ||
    // findAndSendResponse(m, myPid, dataReceivedFromBP)) {
    // return;
    // }

    // if (isDEBUG) {
    // System.out.println("[HANDLE IWANT] I don't have " + m.rowOrColumnNumber +
    // ". I have " + custody2 + " " + custody1 +
    // ". Responding node: " + nodeId + " to " + m.src);
    // }

    // // } else if (distributionStrategy == 2) {
    // // if (findAndSendResponse(m, myPid, custody1Parts) ||
    // // findAndSendResponse(m, myPid, custody2Parts) ||
    // // findAndSendResponse(m, myPid, messageCache.values())) {
    // // return;
    // // }
    // }
    // }

    // // Helper method to search for the requested message and send a response
    // private boolean findAndSendResponse(Message m, int myPid, Collection<Message>
    // messageCollection) {
    // for (Message msg : messageCollection) {
    // if (m.id == msg.id && msg.isRow == m.isRow &&
    // msg.rowOrColumnNumber == m.rowOrColumnNumber &&
    // m.partNumber == msg.partNumber) {

    // Message response = createMessage(m.id, Message.MSG_DATA, nodeId, m.src,
    // m.messageTopicID, (byte[][]) msg.body,
    // m.isRow, m.rowOrColumnNumber, m.partNumber,
    // m.timestamp, m.ackId);

    // storeInEphemeralCache(response);
    // publishMessage(response, m.src, myPid);
    // return true;
    // }
    // }
    // return false;
    // }

    public void handleBlockProducerData(Message m, int myPid) {
        GossipSubProtocol proposerNode = (GossipSubProtocol) CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId);

        incrementDelivered(m.src, m.messageTopicID);

        if (m.src != proposerNode.nodeId)
            return;

        boolean shouldProcess = !messageCache.containsKey(m.id) || IWANTmessageCache.containsKey(m.id);
        if (!shouldProcess)
            return;

        // Manage message cache size
        if (messageCache.size() > MESSAGE_CACHE_SIZE) {
            Long firstMessage = messageCache.keySet().iterator().next();
            messageCache.remove(firstMessage);
        }

        // Store received data
        dataReceivedFromBP.add(createMessage(m.id, m.type, m.src, m.dest, m.messageTopicID, m.body,
                m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.id));
        messageCache.put(m.id, m);

        storeInEphemeralCache(m);

        if (m.body == null) {
            if (isDEBUG) {
                System.out.println("Block proposer received empty data!");
            }
        }

        // Process the received data based on distribution strategy
        if (distributionStrategy == 3 && m.partNumber == -1 && m.rowOrColumnNumber != -1) {
            handleReceivedRowOrCol(m, myPid);
        } else if (distributionStrategy == 2) {
            handleReceivedPart(m, myPid);
        } else {
            return;
        }

        IWANTmessageCache.remove(m.id);
        sendIHaveResponses(m, myPid);
        samplingStarter();
    }

    // Helper method to send IHAVE responses whenever recieved the data coming in
    private void sendIHaveResponses(Message m, int myPid) {
        Message responseWithMetaData = createMessage(m.id, Message.MSG_IHAVE, nodeId, m.dest, m.messageTopicID,
                null, m.isRow, m.rowOrColumnNumber, m.partNumber,
                CommonState.getTime(), -6);

        Message responseWithData = createMessage(m.id, Message.MSG_IHAVE, nodeId, m.dest, m.messageTopicID,
                m.body, m.isRow, m.rowOrColumnNumber, m.partNumber,
                CommonState.getTime(), -6);

        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, nodeId, nodeId);
        gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID, nodeId, nodeId);
    }

    // public void handleData(Message m, int myPid) {
    // if (messageCache.containsKey(m.id) && IWANTmessageCache.containsKey(m.id)) {
    // IWANTmessageCache.remove(m.id);
    // if (distributionStrategy == 3) {
    // handleReceivedRowOrCol(m, myPid);
    // } else if (distributionStrategy == 2) {
    // handleReceivedPart(m, myPid);
    // }
    // samplingStarter();
    // }
    // incrementDelivered(m.src, m.messageTopicID);
    // messageCache.put(m.id, m);
    // // storeInEphemeralCache(m);
    // }

    public void handleData(Message m, int myPid) {
        // If we already cached the data, no need to do anything
        if (messageCache.containsKey(m.id)) {
            // Maybe we only had an "IHAVE placeholder."
            // Let's see if the placeholder's body is null:
            Message placeholder = messageCache.get(m.id);
            if (placeholder.body == null) {
                // fill in the real body now
                placeholder.body = m.body;
                // You can do further logic like "markFirstDelivery" or "incrementDelivered"
            }
            // else we had data already => do nothing
        } else {
            // If we never even had a placeholder, let's store it
            messageCache.put(m.id, m);
        }

        // Then do your normal "apply block or row data" logic
        // e.g. if (distributionStrategy == 3) handleReceivedRowOrCol(m, myPid); etc.
        if (distributionStrategy == 3) {
            handleReceivedRowOrCol(m, myPid);
        } else if (distributionStrategy == 2) {
            handleReceivedPart(m, myPid);
        }

        samplingStarter(); // if you want
    }

    public void markFirstDelivery(Message m) {
        // We only do "first message" logic if we haven't seen it before. E.g.:
        // seenMessageIDs, etc. Then:
        markFirstDelivery(m.src, m.messageTopicID);
    }

    public void markFirstDelivery(BigInteger srcPeer, String topic) {
        PeerScoreInfo psi = peerScores.get(srcPeer);
        if (psi == null)
            return;

        PeerScoreInfo.TopicScores ts = psi.topicScoresMap.computeIfAbsent(topic, k -> new PeerScoreInfo.TopicScores());

        ts.firstMessageDeliveries += 1;
    }

    public void markInvalidMessage(BigInteger srcPeer, String topic) {
        PeerScoreInfo psi = peerScores.get(srcPeer);
        if (psi == null)
            return;

        PeerScoreInfo.TopicScores ts = psi.topicScoresMap.computeIfAbsent(topic, k -> new PeerScoreInfo.TopicScores());

        ts.invalidMessages += 1;
    }

    /**
     * Helper to record that srcPeer delivered a message on a topic.
     * We'll increment their "meshMsgDelivered".
     */
    public void incrementDelivered(BigInteger srcPeer, String topic) {
        PeerScoreInfo psi = peerScores.get(srcPeer);
        if (psi == null) {
            // Make a new PeerScoreInfo if needed
            psi = new PeerScoreInfo(CommonState.getTime());
            peerScores.put(srcPeer, psi);
        }
        PeerScoreInfo.TopicScores ts = psi.topicScoresMap.computeIfAbsent(topic, k -> new PeerScoreInfo.TopicScores());
        ts.meshMsgDelivered += 1;
    }

    public Message createMessage(long id, int type, BigInteger src, BigInteger dest,
            String topicID, Object body,
            boolean isRow, int RowOrColNum,
            int partNum, long timeStamp, long ackid) {
        Message msg;
        if (id == -1) {
            msg = new Message(type, isRow, RowOrColNum, partNum, ackid);
            msg.timestamp = CommonState.getTime();
        } else {
            msg = new Message(id, type, isRow, RowOrColNum, partNum, ackid);
            msg.timestamp = timeStamp;
        }
        msg.src = src;
        msg.dest = dest;
        msg.messageTopicID = topicID;
        msg.body = body;
        return msg;
    }

//    public void sampleDataRequest() {
//        Random random = new Random();
//
//        // Randomly decide whether to sample a row or column
//        boolean isRow = random.nextBoolean();
//        int randomSampleIndex = random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512));
//
//        int rowOrColNo = randomSampleIndex;
//        int noOfRowsAndColsInTopic = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
//
//        // Determine topic number based on configuration
//        int topicNo = (noOfRowsAndColsInTopic == 1)
//                ? (rowOrColNo * 2) + (isRow ? 1 : 2)
//                : rowOrColNo / (noOfRowsAndColsInTopic / 2);
//
//        // Subscribe to the topic
//        Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo));
//        if (topicToSubscribe == null) {
//            if (isDEBUG) {
//                System.out.println("Topic not found: Topic-" + (topicNo));
//            }
//
//            return;
//        }
//        this.subscribeTopic(topicToSubscribe);
//
//        // Select a random node holding the row or column
//        BigInteger destId = selectRandomHolder(isRow, rowOrColNo, random);
//        if (destId == null || destId.equals(this.nodeId)) {
//            if (isDEBUG) {
//                System.out.println("Invalid or self-selection, retrying sample request...");
//            }
//            return; // Avoids recursion, preventing potential stack overflow
//        }
//
//        // Create the sampling request message
//        Message sampleReqMsg = createMessage(
//                -1, Message.MSG_SAMPLE_DATA_REQUEST, this.nodeId, destId,
//                topicToSubscribe.topicID, Integer.toString(randomSampleIndex),
//                isRow, rowOrColNo, -1, 0, -1);
//
//        // Store and send the request
//        this.sentMsg.put(sampleReqMsg.id, sampleReqMsg);
//        noOfSampleRequestsSent++;
//        this.publishMessage(sampleReqMsg, destId, gossipSubId);
//
//        // Schedule timeout for the request
//        scheduleRequestTimeout(sampleReqMsg, destId);
//    }
//    public void startSampling() {
//        IntStream.range(0, sampleAmount).forEach(i -> sampleDataRequest());
//    }

    /* return true when a request is really queued
     * Builds one sampling round.
     * Returns true iff at least one IWANT was queued.
     */
    private boolean sampleDataRequest() {

        final int PARALLEL_ASK = 1;                               //  how many custodians we query at once
//        ThreadLocalRandom rng = ThreadLocalRandom.current();

        Random rng = CommonState.r;


        boolean isRow      = rng.nextBoolean();
        int     shardIndex = rng.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512));

        int rowsColsPerTopic = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
        int topicNo = (rowsColsPerTopic == 1)
                ? (shardIndex * 2) + (isRow ? 1 : 2)
                : shardIndex / (rowsColsPerTopic / 2);

        Topic topic = CustomDistribution.topics.get("Topic-" + topicNo);
        if (topic == null) {
            System.out.printf("Noone hold the shards of topic %d.\n", topicNo);
            return false;
        }
        this.subscribeTopic(topic);

        List<BigInteger> holders = new ArrayList<>(
                isRow ? rowCustodyNodes.get(shardIndex)
                        : columnCustodyNodes.get(shardIndex));
        holders.remove(this.nodeId);
        if (holders.isEmpty()) return false;

        Collections.shuffle(holders, rng);

        int sentNow = 0;
        for (BigInteger dest : holders) {
            if (sentNow == PARALLEL_ASK) break;

            Message req = createMessage(
                    -1,
                    Message.MSG_SAMPLE_DATA_REQUEST,
                    nodeId,
                    dest,
                    topic.topicID,
                    Integer.toString(shardIndex),
                    isRow,
                    shardIndex,
                    -1,
                    0,
                    -1);

            sentMsg.put(req.id, req);
            noOfSampleRequestsSent++;
            publishMessage(req, dest, gossipSubId);
            scheduleRequestTimeout(req, dest);              // timeout is tweaked below

            sentNow++;
        }
        return sentNow > 0;
    }


    public void startSampling() {
        int sent = 0;
        while (sent < sampleAmount) {
            if (sampleDataRequest()) sent++;   // only count real requests
        }
    }


    // Helper method to select a random row or column holder
    private BigInteger selectRandomHolder(boolean isRow, int rowOrColNo, Random random) {
        ArrayList<BigInteger> holders = isRow ? rowCustodyNodes.get(rowOrColNo) : columnCustodyNodes.get(rowOrColNo);
        if (holders == null || holders.isEmpty()) {
            if (isDEBUG) {
                System.out.println("No holders found for " + (isRow ? "row" : "column") + " " + rowOrColNo);
            }
            return null;
        }
        return holders.get(random.nextInt(holders.size()));
    }

    // Helper method to schedule timeout for sample requests
    private void scheduleRequestTimeout(Message sampleReqMsg, BigInteger destId) {
        peersim.GossipSub.Timeout timeout = new peersim.GossipSub.Timeout(1, destId, sampleReqMsg.id);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(sampleReqMsg.dest);

        if (src == null || dest == null) {
            if (isDEBUG) {
                System.out.println("Invalid source or destination for timeout scheduling.");
            }
            return;
        }

        long latency = transport.getLatency(src, dest);
        EDSimulator.add(4 * latency, timeout, src, gossipSubId);
    }

    public void handleSampleRequest(Message m, int myPid) {
        boolean requestedRow = m.isRow;
        int rowOrColNumb = m.rowOrColumnNumber;
        int idx = Integer.parseInt((String) m.body);

        BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId))).nodeId;

        if (!custody1.isEmpty()) {
            for (Message custodyMessage : custodyData1) {
                if (m.isRow == custodyMessage.isRow && m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber
                        && m.partNumber == custodyMessage.partNumber) {
                    byte[][] data = (byte[][]) custodyData1.get(0).body;
                    byte sampleDataResp = data[0][idx];
                    Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, this.nodeId, m.src,
                            m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber,
                            m.partNumber, m.timestamp, m.typeID);
                    this.publishMessage(sampleResponse, m.src, gossipSubId);
                    return;
                }
            }
        }
        if (!custodyData2.isEmpty()) {
            for (Message custodyMessage : custodyData2) {
                if (m.isRow == custodyMessage.isRow && m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber
                        && m.partNumber == custodyMessage.partNumber) {
                    byte[][] data = (byte[][]) custodyData2.get(0).body;
                    byte sampleDataResp = data[0][idx];
                    Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, this.nodeId, m.src,
                            m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber,
                            m.partNumber, m.timestamp, m.typeID);
                    this.publishMessage(sampleResponse, m.src, gossipSubId);
                    return;
                }
            }
        }
    }

    public void handleSampleResponse(Message m, int myPid) {
        sampleArrivalTime.add(CommonState.getTime());
        sampleDelayTime.add(CommonState.getTime() - m.timestamp);
        samplingRTTTimeStore.add(CommonState.getTime());
        noOfSamplesReceived++;
    }

    private void handleTimeOut(peersim.GossipSub.Timeout timeoutEvent, int myPid) {
        if (sentMsg.containsKey(timeoutEvent.msgID)) {
            this.noOfSampleRequestsSent++;
            sampleRequestUnsuccessful++;

            Message sampleMsgSent = sentMsg.get(timeoutEvent.msgID);
            sentMsg.remove(timeoutEvent.msgID);

            BigInteger destId;
//            Random random = new Random();
            Random random = CommonState.r;
            if (sampleMsgSent.isRow) {
                ArrayList<BigInteger> rowHolders = rowCustodyNodes.get(sampleMsgSent.rowOrColumnNumber);
                int sampleHolderNodeIdx = random.nextInt(rowHolders.size());
                destId = rowHolders.get(sampleHolderNodeIdx);
            } else {
                ArrayList<BigInteger> colHolders = columnCustodyNodes.get(sampleMsgSent.rowOrColumnNumber);
                int sampleHolderNodeIdx = random.nextInt(colHolders.size());
                destId = colHolders.get(sampleHolderNodeIdx);
            }

            Message msgToResend = this.createMessage(
                    -1,
                    sampleMsgSent.type,
                    this.nodeId,
                    destId,
                    sampleMsgSent.messageTopicID,
                    sampleMsgSent.body,
                    sampleMsgSent.isRow,
                    sampleMsgSent.rowOrColumnNumber,
                    sampleMsgSent.partNumber,
                    0,
                    sampleMsgSent.typeID);

            sentMsg.put(msgToResend.id, msgToResend);
            publishMessage(msgToResend, msgToResend.dest, myPid);

            peersim.GossipSub.Timeout newTimeout = new peersim.GossipSub.Timeout(1, destId, msgToResend.id);

            Node src = CustomDistribution.networkNodes.get(this.nodeId);
            Node dest = CustomDistribution.networkNodes.get(destId);
            long latency = transport.getLatency(src, dest);

            EDSimulator.add(4 * latency, newTimeout, src, gossipSubId);
        }
    }

    // @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;
        Message m;
        if (!heartbeatScheduled) {
            heartbeatScheduled = true;
            EDSimulator.add(HEARTBEAT_PERIOD, new SimpleEvent(Message.MSG_HEARTBEAT), myNode, myPid);
        }

        switch (((SimpleEvent) event).getType()) {
            case Message.MSG_IHAVE:
                m = (Message) event;
                handleIHave(m, myPid);
                break;

            case Message.MSG_IWANT:
                m = (Message) event;
                handleIWANT(m, myPid);
                break;

            case Message.MSG_DATA:
                m = (Message) event;

                if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                        .getProtocol(gossipSubId))).nodeId) {
                    handleBlockProducerData(m, myPid);
                } else {
                    // normal data
                    // markFirstDelivery(m);
                    handleData(m, myPid);
                }
                break;

            case Message.MSG_BLOCK_PROPOSER:
                System.out.println("I am the block producer");
                m = (Message) event;
                System.out.println("Message sent from trafficGenerator at: " + m.timestamp);
                System.out.println("Block producer started at: " + CommonState.getTime());
                if (distributionStrategy == 3) {
                    nCopiesDistributionStrategy();
                } else if (distributionStrategy == 2) {
                    shardingBasedDistribution();
                }
                EDSimulator.add(HEARTBEAT_PERIOD,
                        new SimpleEvent(Message.MSG_HEARTBEAT), myNode, myPid);
                break;

            case Message.MSG_SAMPLE_DATA_REQUEST:
                m = (Message) event;
                handleSampleRequest(m, myPid);
                break;

            case Message.MSG_SAMPLE_DATA_RESPONSE:
                m = (Message) event;
                if (sentMsg.containsKey(m.id)) {
                    sentMsg.remove(m.id);
                    handleSampleResponse(m, myPid);
                }
                break;

            case peersim.GossipSub.Timeout.TIMEOUT:
                handleTimeOut((peersim.GossipSub.Timeout) event, myPid);
                break;

            case Message.MSG_HEARTBEAT:
//                System.out.println("[HEARTBEAT TEST]");

                heartbeatManager.runHeartbeat(myPid);
                // re-schedule
                EDSimulator.add(HEARTBEAT_PERIOD,
                        new SimpleEvent(Message.MSG_HEARTBEAT), myNode, myPid);
                break;

            case Message.MSG_GRAFT:
                Message graftMsg = (Message) event;
                handleGraft(graftMsg, myPid);
                break;

            case Message.MSG_PRUNE:
                Message pruneMsg = (Message) event;
                handlePrune(pruneMsg, myPid);
                break;

            case Message.MSG_EMPTY:
                break;

        }
    }

    public BigInteger getNodeId() {
        return this.nodeId;
    }

    /**
     * set the current NodeId
     *
     * @param tmp BigInteger
     */
    public void setNodeId(BigInteger tmp) {
        this.nodeId = tmp;
    }

    public void setHeartbeatManager(HeartbeatManager hm) {
        this.heartbeatManager = hm;
    }

    // constants
    private static final int MAX_DIMENSION_SIZE = 512;
    private static final int MESSAGE_TYPE = 3;

    private Message createRowOrColumnMessageForDistribution(
            int copiesSent,
            Message baseMessage,
            Block block,
            int rowOrColumnIndex,
            String topicId,
            BigInteger destinationId,
            boolean isRow) {
        // Only build a new 'baseMessage' if this is the first copy
        if (copiesSent == 0) {
//            int halfRows = Configuration.getInt("NUMBER_OF_ROWS") / 2;
            int halfRows = Configuration.getInt("NUMBER_OF_ROWS");
            // Determine which data need to send (row or column).
            // For example, if rows are large, you're only sending half of it here.
            byte[][] dataToSend = isRow
                    ? Arrays.copyOfRange(block.getRowData(rowOrColumnIndex), 0, halfRows)
                    : Arrays.copyOfRange(block.getColumnData(rowOrColumnIndex), 0, halfRows);

            // Create the initial message that others will copy from
            baseMessage = new Message(
                    MESSAGE_TYPE,
                    dataToSend,
                    /* isValid? */ true,
                    rowOrColumnIndex,
                    /* other args */ -1,
                    -1);
            baseMessage.src = this.getNodeId();
            baseMessage.messageTopicID = topicId;
            baseMessage.dest = destinationId;
        }

        // Create an actual message to send out (same data as baseMessage)
        Message messageToSend = createMessage(
                baseMessage.id,
                MESSAGE_TYPE,
                this.nodeId,
                destinationId,
                topicId,
                baseMessage.body,
                isRow,
                rowOrColumnIndex,
                -1,
                -1,
                -1);

        System.out.println("I am holding "
                + (isRow ? "row" : "column")
                + " " + rowOrColumnIndex
                + " for " + destinationId);

        // Publish/send the message
        publishMessage(messageToSend, destinationId, gossipSubId);

        return baseMessage;
    }

    private void nCopiesDistributionStrategy() {
        // Load all the relevant configuration values
        int numberOfCopiesToSend = Configuration.getInt("NUMBER_COPIES_DISTRIBUTED");
        int numberOfRowsAndColsInTopic = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC");

        int rowIndex = 0; // Tracks which row are distributing
        int columnIndex = 0; // Tracks which column are distributing

        // Retrieve the block to distribute
        Block b = block;

        // Just an example usage to log the block proposer’s ID
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));
        System.out.println("Block proposer id is ------: " + iGossipBlockProposer.getNodeId());

        boolean isRowTopic = true;
        boolean isColTopic = false;

        // Iterate over each topic in the distribution
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            System.out.println();
            System.out.println();
            System.out.println("****");
            System.out.println(topicEntry.getKey());

            Topic currentTopic = topicEntry.getValue();

            int distributionCount = 0; // 'cnt' in original code
            int nodeCounter = 0;
            int copiesSent = 0;

            Message baseMessage = null;

            // Iterate over all nodes that are subscribed to this topic
            for (Node node : currentTopic.topicMembers) {
                GossipSubProtocol gossipProtocol = (GossipSubProtocol) node.getProtocol(gossipSubId);
                // If the topic size is more than 1, decide if are distributing rows or
                // columns
                if (numberOfRowsAndColsInTopic != 1) {
                    if (distributionCount < (numberOfRowsAndColsInTopic / 2)) {
                        isRowTopic = true;
                    } else {
                        isRowTopic = false;
                    }
                }

                // If distributed enough rows/columns for this topic, break
                if (distributionCount >= numberOfRowsAndColsInTopic) {
                    break;
                }

                // If already sent the desired number of copies
                // AND the nodeCounter is not at the boundary for a new row/column holder
                if (copiesSent >= numberOfCopiesToSend
                        && (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC != 0)) {
                    nodeCounter++;
                    // Once hit the boundary again, reset things
                    if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                        copiesSent = 0;
                        if (isRowTopic) {
                            rowIndex++;
                        } else {
                            columnIndex++;
                        }
                        distributionCount++;
                    }
                    continue;
                }

                // Check row/column limits
                if (isRowTopic && rowIndex == MAX_DIMENSION_SIZE) {
                    // If reached the limit, move on
                    continue;
                }
                if (!isRowTopic && columnIndex == MAX_DIMENSION_SIZE) {
                    // If reached the limit, end the distribution entirely
                    return;
                }

                // Determine which index are distributing (row or column)
                int currentIndex = isRowTopic ? rowIndex : columnIndex;
                BigInteger destinationId = gossipProtocol.getNodeId();

                // Create/send the message
                Message newMsg = createRowOrColumnMessageForDistribution(
                        copiesSent,
                        baseMessage,
                        b,
                        currentIndex,
                        currentTopic.topicID,
                        destinationId,
                        isRowTopic);

                // If this was the first time sent a copy, record it as our base
                if (copiesSent == 0) {
                    baseMessage = newMsg;
                }

                nodeCounter++;
                copiesSent++;

                // If reached the boundary for holders, reset counters
                if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                    copiesSent = 0;
                    if (isRowTopic) {
                        rowIndex++;
                    } else {
                        columnIndex++;
                    }
                    distributionCount++;
                }

                // Once again, if hit our max column limit, return
                if (!isRowTopic && columnIndex == MAX_DIMENSION_SIZE) {
                    return;
                }
            }

            // Flip row/column topic for the next set
            isRowTopic = !isRowTopic;
            isColTopic = !isColTopic;
        }

        // Logging final counters and stats
        System.out.println(rowIndex);
        System.out.println(columnIndex);
        System.out.println("*********Block proposer has sent the messages ********");
        System.out.println("Data sent size " + totalDataTransmitted);
        System.out.println("Data transmission time " + totalTransmissionTime);
    }

    private void shardingBasedDistribution() {
        // Number of “shards” or subdivisions
        int numberOfDivisions = NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC;

        // Total size of each row or column
        int numberOfCellsInRowOrColumn = Configuration.getInt("NUMBER_OF_ROWS");
        // Each part is this many cells
        int partSize = numberOfCellsInRowOrColumn / numberOfDivisions;

        // Retrieve the block to distribute
        Block b = block;

        // Log the block proposer ID
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));
        System.out.println("Block proposer id is ------: " + iGossipBlockProposer.getNodeId());
        System.out.println("Data sent size " + totalDataTransmitted);

        // Track which row/column distributing
        int rowIndex = 0;
        int columnIndex = 0;

        boolean isRowTopic = true;
        boolean isColTopic = false;

        // Go through each topic to do sharding-based distribution
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {

            System.out.println();
            System.out.println();
            System.out.println("****");
            System.out.println(topicEntry.getKey());

            Topic currentTopic = topicEntry.getValue();

            // Counters to track distribution across this topic
            int distributionCount = 0; // Originally 'cnt'
            int nodeIndex = 0; // Originally 'nodeCounter'
            int copiesSent = 0; // Originally 'numberOfCopiesSent'

            // For all nodes in this topic
            for (Node node : currentTopic.topicMembers) {
                GossipSubProtocol gossipProtocol = (GossipSubProtocol) node.getProtocol(gossipSubId);

                // If multiple rows/columns can exist in a single topic, figure out if we’re
                // distributing rows or columns
                int rowsAndColsInTopic = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC");
                if (rowsAndColsInTopic != 1) {
                    // Distribute the first half as rows, then the second half as columns
                    isRowTopic = (distributionCount < rowsAndColsInTopic / 2);
                }

                // If already assigned enough rows/columns for this topic, stop.
                if (distributionCount >= rowsAndColsInTopic) {
                    break;
                }

                // If hit the maximum row/column limit, move on or stop.
                if (isRowTopic && rowIndex == MAX_DIMENSION_SIZE) {
                    continue;
                }
                if (!isRowTopic && columnIndex == MAX_DIMENSION_SIZE) {
                    // The original code returns entirely if columns are maxed out.
                    return;
                }

                // Prepare the data to send (shard of row or column)
                BigInteger destId = gossipProtocol.getNodeId();
                byte[][] partToSend;
                if (isRowTopic) {
                    partToSend = Arrays.copyOfRange(
                            b.getRowData(rowIndex),
                            copiesSent * partSize,
                            (copiesSent * partSize) + partSize);
                    System.out.println("[SHARDING DISTRIBUTION: ]Row number " + rowIndex + " part " + copiesSent
                            + " -> " + destId);

                } else {
                    partToSend = Arrays.copyOfRange(
                            b.getColumnData(columnIndex),
                            copiesSent * partSize,
                            (copiesSent * partSize) + partSize);
                    System.out.println("[SHARDING DISTRIBUTION: ]Column number " + columnIndex + " part "
                            + copiesSent + " -> " + destId);

                }

                // Build and send the message for this shard
                Message messageToSend = createMessage(
                        /* messageID */ -1,
                        MESSAGE_TYPE,
                        this.nodeId,
                        destId,
                        currentTopic.topicID,
                        partToSend,
                        /* isRow? */ isRowTopic,
                        isRowTopic ? rowIndex : columnIndex,
                        copiesSent,
                        -1,
                        -1);
                publishMessage(messageToSend, destId, gossipSubId);

                nodeIndex++;
                copiesSent++;

                // When reach the boundary of row/column holders, move to the next row/column
                if (nodeIndex % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                    copiesSent = 0;
                    if (isRowTopic) {
                        rowIndex++;
                    } else {
                        columnIndex++;
                    }
                    distributionCount++;
                }

                // If columns max out, stop entirely
                if (!isRowTopic && columnIndex == MAX_DIMENSION_SIZE) {
                    return;
                }
            }

            // Toggle row vs column topics after finishing one pass
            isRowTopic = !isRowTopic;
            isColTopic = !isColTopic;
        }

        // Final logs
        System.out.println(rowIndex);
        System.out.println(columnIndex);
        System.out.println("*********Block proposer has sent the messages ********");
        System.out.println("Data sent size " + totalDataTransmitted);
        System.out.println("Data transmission time " + totalTransmissionTime);
    }
}