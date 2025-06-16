package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;
import peersim.util.IncrementalStats;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static peersim.GossipSub.CustomDistribution.*;
import static peersim.GossipSub.GossipScoringConfig.TOPIC_PARAMS;

public class GossipSubProtocol implements Cloneable, EDProtocol {

    private static final long HEARTBEAT_PERIOD = 1000; // 1 second

    // --- Adaptive Gossip tracer (v1.1) ---
    public static final boolean USE_ADAPTIVE_GOSSIP =
            Configuration.getBoolean("USE_ADAPTIVE_GOSSIP", false);

    private static final int D_LAZY = Configuration.getInt("D_LAZY", 6);
    public final Map<BigInteger,GossipTracer> gossipTracer = new HashMap<>();
    public static final double TRACER_DECAY =
            Configuration.getDouble("ADAPTIVE_GOSSIP_DECAY", 0.5);
    public static final double TRACER_TH   =
            Configuration.getDouble("ADAPTIVE_GOSSIP_THRESHOLD", 0.3);


    static class GossipTracer {
        int ihaveSeen  = 0;     // adverts received from that peer
        int iwantSent  = 0;     // IWANTs we had to send to that peer
        void decay(double f) {               // exponential decay every heartbeat
            ihaveSeen = (int) (ihaveSeen * f);
            iwantSent = (int) (iwantSent * f);
        }
    }

    // Counter per tick
    public long iHaveSent = 0;
    public long iWantRecv = 0;
    public ArrayList<Long> iHaveSentAtT = new ArrayList<>();
    public ArrayList<Long> iWantRecvAtT = new ArrayList<>();
    private long _lastIHaveSent = 0, _lastIWantRecv = 0;



    private Map<Long, Long> lastAdvertisedTime = new HashMap<>();
    private static final long ADVERTISEMENT_TTL = 5000;

    private boolean heartbeatScheduled = false;

    // Configuration Constants
    private static int MESSAGE_CACHE_SIZE = 1024;
    private static String PAR_TRANSPORT = "transport";
    private static int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
    private static int NUMBER_OF_VALIDATORS_PER_TOPIC = Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC", 128);
//    private static int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC = NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1
//            ? NUMBER_OF_VALIDATORS_PER_TOPIC
//            : (int) Math.ceil(
//                    (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));
    private static final int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC =
            Configuration.getInt("ROW_OR_COLUMN_HOLDERS_PER_TOPIC",
                    /* fallback: keep old heuristic */
                    (NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1)
                            ? NUMBER_OF_VALIDATORS_PER_TOPIC
                            : (int)Math.ceil((NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) /
                            Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC")));

    private static final double GOSSIP_FACTOR = Configuration.getDouble("GOSSIP_FACTOR", 0.33);
    private static final long SAMPLE_REQ_TIMEOUT = Configuration.getLong("SAMPLE_REQ_TIMEOUT", 4000); // 4 s fallback

    private static final long SLOT_DURATION = Configuration.getLong("SLOT_DURATION", 12000); // 12 s fallback

    public static String prefix;

    // Instance Variables
    public BigInteger nodeId;
    public UnreliableTransport transport;
    public int tid;
    private int gossipSubId;
    public int D_LOW = Configuration.getInt("MIN_DEGREE", 4);
    public int D_HIGH = Configuration.getInt("MAX_DEGREE", 8);
    protected int D = Configuration.getInt("DEGREE", 8);;

    // ── sharding‐specific tunables
    // ────────────────────────────────────────────────
    private static final int SHARD_COPIES = Configuration.getInt("SHARD_COPIES", 1);

    public Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> meshPeersByTopic = new HashMap<>();
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
    public int duplicateIHaveMessage = 0;
    public int duplicateData = 0;

    private final java.util.Set<Long> pendingIWant = new java.util.HashSet<>();

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
    private boolean isMaliciousNode = false; // Ommision

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

    private void recordTickCounters() {
        int t = (int) CommonState.getTime();

        while (iHaveSentAtT.size() <= t) { // grow only the 2 arrays
            iHaveSentAtT.add(0L);
            iWantRecvAtT.add(0L);
        }
        iHaveSentAtT.set(t, iHaveSent - _lastIHaveSent);
        iWantRecvAtT.set(t, iWantRecv - _lastIWantRecv);

        _lastIHaveSent = iHaveSent;
        _lastIWantRecv = iWantRecv;
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

    public double computeScore(PeerScoreInfo psi) {
        long nowMs = CommonState.getTime();
        double total = 0.0;

        for (Map.Entry<String, PeerScoreInfo.TopicScores> e : psi.topicScoresMap.entrySet()) {
            String t = e.getKey();
            PeerScoreInfo.TopicScores ts = e.getValue();
            GossipScoringConfig.TopicParam p = TOPIC_PARAMS.getOrDefault(t, GossipScoringConfig.DEFAULT_TOPIC_PARAM);

            /* a) time-in-mesh in **seconds** */
            double secs = (nowMs - psi.timeInMeshStart) / 1000.0;
            double timeScore = p.timeInMeshWeight * Math.min(secs, p.timeInMeshCapSeconds);

            /* first deliveries */
            double first = p.firstMsgWeight *
                    Math.min(ts.firstMessageDeliveries, p.firstMsgCap);

            /* b) mesh deliveries reward / penalty around threshold */
            double meshDelivered = Math.min(ts.meshMsgDelivered, p.meshDeliveriesCap);
            double meshScore = (meshDelivered < p.meshDeliveriesThreshold)
                    ? p.meshDeliveriesWeightUnder *
                            (p.meshDeliveriesThreshold - meshDelivered)
                    : p.meshDeliveriesWeightOver *
                            (meshDelivered - p.meshDeliveriesThreshold);

            /* invalid msgs */
            double invalid = p.invalidMsgWeight * ts.invalidMessages;

            total += p.topicWeight * (timeScore + first + meshScore + invalid);
        }
        psi.cachedScore = total;
        return total;
    }

    private double score(BigInteger p) {
        GossipTracer gt = gossipTracer.get(p);
        if (gt == null || gt.ihaveSeen == 0) return 1.0;   // no history → be fair
        return (double) gt.iwantSent / gt.ihaveSeen;       // 0 … 1
    }



    public void advertiseMessageIHAVE(Message originalMsg, int myPid) {
        Set<BigInteger> peers = meshPeersByTopic.getOrDefault(originalMsg.messageTopicID, Collections.emptySet());
        if (peers.isEmpty())
            return;

//        int advertiseCnt = Math.max(1, (int) Math.ceil(GOSSIP_FACTOR * peers.size()));
        int advertiseCnt = USE_ADAPTIVE_GOSSIP        // ← the flag you added before
                ? Math.min(D_LAZY, peers.size())      // fixed fan-out in adaptive mode
                : Math.max(1, (int) Math.ceil(GOSSIP_FACTOR * peers.size()));
//        List<BigInteger> shuffled = new ArrayList<>(peers);
        List<BigInteger> picks = new ArrayList<>(peers);

        if (USE_ADAPTIVE_GOSSIP) {
            picks.sort((p,q) ->          /* descending by “usefulness” ratio */
                    Double.compare(score(q), score(p)));
        }

        Collections.shuffle(picks.subList(
                        USE_ADAPTIVE_GOSSIP ? Math.min(peers.size(), 4) : 0, picks.size()),
                CommonState.r);
//        Collections.shuffle(shuffled, CommonState.r);

        Message advert = createMessage(originalMsg.id, Message.MSG_IHAVE, nodeId, null,
                originalMsg.messageTopicID, null,
                originalMsg.isRow, originalMsg.rowOrColumnNumber,
                originalMsg.partNumber, CommonState.getTime(), -6);

        for (int i = 0; i < advertiseCnt; i++) {
            BigInteger peer = picks.get(i);
            if (peer.equals(nodeId))
                continue;
            Message copy = (Message) advert.copy();
            copy.dest = peer;
            publishMessage(copy, peer, myPid);
        }
    }

    public void updateMeshConnections() {
        if (subscribedTopics.isEmpty())
            return;

        for (String topicID : subscribedTopics.stream().map(t -> t.topicID).collect(Collectors.toList())) {
            meshPeersByTopic.putIfAbsent(topicID, new HashSet<>());
            Set<BigInteger> peers = meshPeersByTopic.get(topicID);
            if (peers == null)
                continue;

            if (peers.size() < D_LOW) {
                addMorePeers(topicID, D - peers.size());
            }
            if (peers.size() > D_HIGH) {
                removeExcessPeers(topicID, peers.size() - D);
            }
        }
    }

    public boolean inMyMesh(String topicID, BigInteger peer) {
        Set<BigInteger> mesh = meshPeersByTopic.get(topicID);
        return mesh != null && mesh.contains(peer);
    }

    // Function to add new peers when the mesh is too small
//    public void addMorePeers(String topicID, int needed) {
//        Topic topic = CustomDistribution.topics.get(topicID);
//        if (topic == null)
//            return;
//
//        List<Node> potentialPeers = new ArrayList<>(topic.topicMembers);
//        Collections.shuffle(potentialPeers, CommonState.r);
//
//        for (Node newPeer : potentialPeers) {
//            // If we've already reached our local 'degree', stop adding more
//            if (localMesh.get(topicID).size() >= degree) {
//                break;
//            }
//
//            if (needed <= 0)
//                break;
//
//            GossipSubProtocol peerNode = (GossipSubProtocol) newPeer.getProtocol(gossipSubId);
//
//            // If that peer already has that node, skip
//            // (only if you want to reduce double-link creation)
//            if (peerNode.localMesh.get(topicID).contains(this.nodeId)) {
//                continue;
//            }
//
//            // We add them to OUR local mesh
//            localMesh.putIfAbsent(topicID, new HashSet<>());
//            localMesh.get(topicID).add(peerNode.nodeId);
//
//            addPeerScoreIfAbsent(peerNode.nodeId);
//
//            // Then we send them a GRAFT, so *they* can decide if they want us in their mesh
//            Message graft = createMessage(
//                    -1,
//                    Message.MSG_GRAFT,
//                    this.nodeId,
//                    peerNode.nodeId,
//                    topicID,
//                    null,
//                    false, -1, -1,
//                    CommonState.getTime(),
//                    -1);
//            publishMessage(graft, peerNode.nodeId, gossipSubId);
//
//            // We used up one slot
//            needed--;
//
//            // If we want to be REALLY sure we don't overshoot, we do:
//            if (localMesh.get(topicID).size() >= degree) {
//                break;
//            }
//        }
//    }

    private void addMorePeers(String topicID, int needed) {
        Topic topic = CustomDistribution.topics.get(topicID);
        if (topic == null) return;

        List<Node> shuffled = new ArrayList<>(topic.topicMembers);
        Collections.shuffle(shuffled, CommonState.r);

        for (Node n : shuffled) {
            if (meshPeersByTopic.get(topicID).size() >= D_LOW || needed <= 0) break;

            GossipSubProtocol peer = (GossipSubProtocol) n.getProtocol(gossipSubId);
            if (peer.meshPeersByTopic.get(topicID).contains(this.nodeId)) continue;

            /* graft */
            meshPeersByTopic.get(topicID).add(peer.nodeId);
            addPeerScoreIfAbsent(peer.nodeId);

            Message graft = createMessage(-1, Message.MSG_GRAFT,
                    this.nodeId, peer.nodeId,
                    topicID, null, false, -1, -1,
                    CommonState.getTime(), -1);
            publishMessage(graft, peer.nodeId, gossipSubId);
            needed--;
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
        meshPeersByTopic.putIfAbsent(topicID, new HashSet<>());
        meshPeersByTopic.get(topicID).add(p);

        // Just in case, ensure we track peer's score info
        addPeerScoreIfAbsent(p);

        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] Node " + nodeId
                    + " accepted peer " + p
                    + " into mesh for topic=" + topicID
                    + ". Current mesh size=" + meshPeersByTopic.get(topicID).size());
        }

        // If oversubscribed, remove some peers
        if (meshPeersByTopic.get(topicID).size() > D) {
            int over = meshPeersByTopic.get(topicID).size() - D; // trim back to “degree”
            if (isDEBUG) {
                System.out.printf("[DEBUG handleGraft] mesh %s oversized (%d>%d); pruning %d peers%n",
                        topicID, meshPeersByTopic.get(topicID).size(), D_HIGH, over);
            }
            removeExcessPeers(topicID, over);
        }
    }

    public void handlePrune(Message m, int myPid) {
        BigInteger pruner = m.src; // the node that is pruning me
        String topicID = m.messageTopicID;
        long now = CommonState.getTime();

        // Log that received a PRUNE
        if (isDEBUG) {
            System.out.println("[DEBUG handlePrune] Node " + nodeId
                    + " received PRUNE from " + pruner
                    + " for topic=" + topicID
                    + " at time=" + now);
        }

        // Remove that node from local mesh (if present).
        Set<BigInteger> meshPeers = meshPeersByTopic.getOrDefault(topicID, new HashSet<>());
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

        if (sizeAfterRemoval < D_LOW) {
            int needed = D - sizeAfterRemoval;
            if (needed > 0) {
                if (isDEBUG) {
                    System.out.println("[DEBUG handlePrune] localMesh[" + topicID + "] too small ("
                            + sizeAfterRemoval + " < minDegree=" + D_LOW
                            + "). GRAFTing " + needed + " peers...");
                }
                addMorePeers(topicID, needed);
            }
        }

        // If for some reason end up bigger than maxDegree, removeExcessPeers
        if (sizeAfterRemoval > D_HIGH) {
            int over = sizeAfterRemoval - D;
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] localMesh[" + topicID + "] oversubscribed size="
                        + sizeAfterRemoval + " > maxDegree=" + D_HIGH
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

        // Remove from local mesh
        Set<BigInteger> meshPeers = meshPeersByTopic.getOrDefault(topicID, new HashSet<>());
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

//    private void removeExcessPeers(String topicID, int count) {
//        Set<BigInteger> peers = localMesh.get(topicID);
//        if (peers == null || count <= 0)
//            return;
//
//        int maxAllowed = peers.size() - minDegree;
//        count = Math.min(count, maxAllowed);
//        if (count <= 0)
//            return;
//
//        // Make sure each peer has a PeerScoreInfo
//        for (BigInteger p : peers) {
//            addPeerScoreIfAbsent(p);
//        }
//
//        // Then do the sorting:
//        List<BigInteger> sorted = new ArrayList<>(peers);
//        sorted.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));
//
//        // Prune the worst 'count' peers
//        for (int i = 0; i < count; i++) {
//            BigInteger toRemove = sorted.get(i);
//            prunePeer(toRemove, topicID);
//        }
//    }

    private void removeExcessPeers(String topicID, int toPrune) {
        Set<BigInteger> mesh = meshPeersByTopic.get(topicID);
        if (mesh == null || toPrune <= 0) return;

        /* ensure scores exist, then sort by ascending score */
        mesh.forEach(this::addPeerScoreIfAbsent);
        List<BigInteger> sorted = new ArrayList<>(mesh);
        sorted.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));

        /* PRUNE the ‘toPrune’ worst peers */
        for (int i = 0; i < toPrune && i < sorted.size(); i++) {
            prunePeer(sorted.get(i), topicID);
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

    public double getAverageBandwidthKBps() {
        if (totalTransmissionTime == 0) // nothing sent yet
            return 0.0;

        double kiloBytes = totalDataTransmitted / 1024.0; // B → kB
        double seconds = totalTransmissionTime / 1000.0; // ms→s
        return kiloBytes / seconds;
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
            meshPeersByTopic.remove(topic.topicID);
            return;
        }
    }

    public boolean isSubscribedToTopic(Topic topic) {
        return subscribedTopics.contains(topic);
    }

    public void setTopicMembersList(String topicName, Set<BigInteger> members) {
        topicNodes.put(topicName, members);
    }

    private Message lookupInCustody(Message want) {
        for (Message c : custodyData1)
            if (c.id == want.id)
                return c;
        for (Message c : custodyData2)
            if (c.id == want.id)
                return c;
        return null;
    }

    public void publishMessage(Message m, BigInteger destId, int myPid) {

        // Malicious
        // isMaliciousNode() && !m.src.equals(this.nodeId) // forwarding only ommisionb
        if (isMaliciousNode()) { // Fully omission -> not receiving or fowarding any messgae
            if (isDEBUG) {
                System.out.println("[MALICIOUS‑DROP] node " + nodeId +
                        " dropped fwd of msg " + m.id);
            }
            return;
        }

        // If this node *originated* the message and it already contains the body,
        // make sure a copy is in messageCache so later IWANTs can be served.
        if (m.src != null && m.src.equals(this.nodeId) && m.body != null) {
            messageCache.putIfAbsent(m.id, (Message) m.copy());
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
            Message newMessage = createMessage(
                    m.id, m.type, src, peerId, m.messageTopicID,
                    m.body, m.isRow, m.rowOrColumnNumber, m.partNumber,
                    CommonState.getTime(), (m.typeID == -6) ? m.typeID : m.id);
            publishMessage(newMessage, peerId, myPid);
        }
    }

    public void sendMessageToPeers(Message m, int myPid, String topicID,
            BigInteger avoid, BigInteger src) {
        Set<BigInteger> peers = meshPeersByTopic.get(topicID);
        if (peers == null || peers.isEmpty()) {
            if (isDEBUG) {
                System.out.println("[ERROR SEND MESSAGE: ] local mesh empty for node: " + nodeId);
            }
            return;
        }
        sendMessageToTopicNodes(m, myPid, topicID, meshPeersByTopic, avoid, src);
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
//         int threshold = (NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC + 1) / 2;
        int threshold = 1;

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

        /** Deep-compare two possible payload objects (byte[], byte[][], String, …). */
                private static boolean samePayload(Object a, Object b) {
                if (a == b) return true;
                if (a == null || b == null) return false;
                if (a instanceof byte[] && b instanceof byte[])
                        return java.util.Arrays.equals((byte[]) a, (byte[]) b);
                if (a instanceof byte[][] && b instanceof byte[][])
                        return java.util.Arrays.deepEquals((Object[]) a, (Object[]) b);
                return a.equals(b);
            }

    private void processCustody(Message m, List<Message> custodyParts, int threshold) {

        if (custodyParts.contains(m)) {
            if (isDEBUG) {
                System.out.printf("[DUP] node %s got duplicate shard id=%d at t=%d%n",
                        nodeId, m.id, CommonState.getTime());
            }

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
            if (isDEBUG) {
                System.out.printf("[DUP] node %s got duplicate shard id=%d at t=%d%n",
                        nodeId, m.id, CommonState.getTime());
            }
        }
    }

    // private void processCustody(Message m, List<Message> custodyParts, int
    // threshold) {
    // if (custodyParts.size() < threshold) {
    // long currentTime = CommonState.getTime();
    // seedPartArrivalTimeFromPeer.add(currentTime);
    // seedPartDelayTimeFromPeer.add(currentTime - m.timestamp);
    // seedPartArrivalTimeStore.add(currentTime - m.timestamp);
    // custodyParts.add(m);
    //
    // if (custodyParts.size() == threshold) {
    // createWholeRowOrColumnSeed(m, m.rowOrColumnNumber);
    // }
    // }
    // }

    // public void handleReceivedRowOrCol(Message m, int myPid) {
    // String s = m.isRow ? "row" : "column";
    // String key = s + m.rowOrColumnNumber;
    //
    // if (custody1.equals(key)) {
    // custodyData1.add(m);
    // } else if (custody2.equals(key)) {
    // custodyData2.add(m);
    // } else {
    // return;
    // }
    //
    // long currentTime = CommonState.getTime();
    // messageArrivalTimeFromBP.add(currentTime);
    // messageDelayTimeFromBP.add(currentTime - m.timestamp);
    // seedArrivalTimeStore.add(currentTime);
    // }

    public void handleReceivedRowOrCol(Message m, int myPid) {
        String key = (m.isRow ? "row" : "column") + m.rowOrColumnNumber;

        if (custody1.equals(key)) {
            if (custodyData1.contains(m)) {
                if (isDEBUG) {
                    System.out.printf("[DUP] node %s got duplicate shard id=%d at t=%d%n",
                            nodeId, m.id, CommonState.getTime());
                }
            } else {
                custodyData1.add(m); // first time we see it
            }

        } else if (custody2.equals(key)) { // message belongs to custody‑2
            if (custodyData2.contains(m)) {
                if (isDEBUG) {
                    System.out.printf("[DUP] node %s got duplicate shard id=%d at t=%d%n",
                            nodeId, m.id, CommonState.getTime());
                }
            } else {
                custodyData2.add(m);
            }

        } else { // not my custody – ignore
            return;
        }

        long now = CommonState.getTime();
        messageArrivalTimeFromBP.add(now);
        messageDelayTimeFromBP.add(now - m.timestamp);
        seedArrivalTimeStore.add(now);
    }

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
            // handle duplicate I have
            duplicateIHaveMessage++;
            if (IWANTmessageCache.containsKey(m.id) && m.body != null) { // Late-arriving message
                processReceivedMessage(m, myPid);
            }
            return;
        }
        messageCache.put(m.id, m);
        if (USE_ADAPTIVE_GOSSIP) {
            gossipTracer.computeIfAbsent(m.src, k -> new GossipTracer()).ihaveSeen++;
        }
        if (m.typeID == -6) {
            handleTypeMessage(m, myPid);
        }

        messageCache.put(m.id, m);

        long now = CommonState.getTime();
            if (m.body == null && !IWANTmessageCache.containsKey(m.id)) {
                    Message want = createMessage(
                                    m.id, Message.MSG_IWANT,
                                    nodeId,            /* src  = me      */
                                    m.src,             /* dest = advert sender */
                                    m.messageTopicID,
                                    /* body = */ null,
                                    m.isRow, m.rowOrColumnNumber, m.partNumber,
                                    now, -6);

                            publishMessage(want, m.src, myPid);
                    IWANTmessageCache.put(m.id, m);   // remember so we send it only once
                }

        storeInEphemeralCache(m);

        if (lastAdvertisedTime.containsKey(m.id)
                && (now - lastAdvertisedTime.get(m.id) < ADVERTISEMENT_TTL)) {

            // Node already gossiped this IHAVE not too long ago => skip re-gossip
            return;
        }

        lastAdvertisedTime.put(m.id, now);

        // Metadata
        Message advert = createMessage(m.id, Message.MSG_IHAVE, m.src,
                m.dest, m.messageTopicID, /* body */ null,
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), -6);

        // Full Message
        Message full = createMessage(m.id, Message.MSG_DATA, nodeId,
                m.dest, m.messageTopicID, m.body,
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), -6);


        incrementDelivered(m.src, m.messageTopicID);
        // edger push
        sendMessageToPeers(full, myPid, m.messageTopicID, this.nodeId, m.src);

        // Lazy gossip
//        gossipMessageToTopicNodes(advert, myPid, m.messageTopicID, this.nodeId, m.src);
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
        if (USE_ADAPTIVE_GOSSIP)
            gossipTracer.computeIfAbsent(m.dest,k->new GossipTracer()).iwantSent++;

        Message request = createMessage(m.id, Message.MSG_IWANT, nodeId, m.src, m.messageTopicID, "",
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), m.typeID);

        IWANTmessageCache.put(m.id, m);
        publishMessage(request, m.src, myPid);
    }

//    public void handleIWANT(Message m, int myPid) {
//        if (isDEBUG) {
//            System.out.println("[DEBUG handleIWANT] Node " + nodeId
//                    + " received IWANT for msgId=" + m.id
//                    + " from node " + m.src);
//        }
//        iWantRecv++;
//
//        // Try to find the requested message among your local caches/collections.
//        // For example, if you store complete messages in 'messageCache', do:
//        Message storedMsg = messageCache.get(m.id);
//
//        // If we don't have it, do nothing (or log).
//        // The requesting node may ask another peer.
//        if (storedMsg == null || storedMsg.body == null) {
//            if (isDEBUG) {
//                System.out.println("[DEBUG handleIWANT] Node " + nodeId
//                        + " => does NOT have msgId=" + m.id + ", ignoring request.");
//            }
//            return;
//        }
//
//        // Message storedMsg = messageCache.get(m.id);
//        if (storedMsg == null || storedMsg.body == null) {
//            storedMsg = lookupInCustody(m);
//            if (storedMsg == null) { // still nothing
//                if (isDEBUG) {
//                    System.out.println("[DEBUG handleIWANT] Node " + nodeId +
//                            " cannot satisfy msgId=" + m.id + " — ignoring");
//                }
//                return;
//            }
//        }
//
//        // We do have it. Construct a MSG_DATA response with the full body.
//        // Note: we re-use m.id, plus 'nodeId' as our src, and 'm.src' as the dest.
//        // The body is the actual payload from 'storedMsg'.
//        Message response = createMessage(
//                m.id,
//                Message.MSG_DATA,
//                nodeId, // from me
//                m.src, // back to the requester
//                storedMsg.messageTopicID,
//                storedMsg.body, // the actual payload
//                storedMsg.isRow,
//                storedMsg.rowOrColumnNumber,
//                storedMsg.partNumber,
//                CommonState.getTime(), // timestamp
//                m.id // ackId can track which message we're responding to
//        );
//
//        // (Optional) store it in ephemeral cache if your logic requires
//        storeInEphemeralCache(response);
//
//        // Publish/forward the full data back to the requester
//        publishMessage(response, m.src, myPid);
//
//        if (isDEBUG) {
//            System.out.println("[DEBUG handleIWANT] Node " + nodeId
//                    + " => Sent MSG_DATA for msgId=" + m.id
//                    + " to node " + m.src);
//        }
//    }


    public void handleIWANT(Message m, int myPid) {

        /* ----------  logging & accounting  ---------- */
        if (isDEBUG) {
            System.out.printf("[DEBUG IWANT] t=%d  node=%s  ← IWANT msgId=%d from %s%n",
                    CommonState.getTime(), nodeId, m.id, m.src);
        }
        iWantRecv++;                                   // stats counter

        /* ---------- look up the requested message ---------- */
        Message stored = messageCache.get(m.id);       // primary cache

        if (stored == null || stored.body == null) {   // fallback: long-term custody
            stored = lookupInCustody(m);               // returns null if not found
        }

        /* ---------- if cannot satisfy the request? ---------- */
        if (stored == null || stored.body == null) {
            if (isDEBUG) {
                System.out.printf("[DEBUG IWANT] node=%s  cannot satisfy msgId=%d – ignored%n",
                        nodeId, m.id);
            }
        /* Optional: down-score ourselves for “broken promise”
           or penalise the requester if the msgId was never advertised */
            // markInvalidMessage(m.src, m.messageTopicID);
            return;                                    // give up for now
        }

        /* ---------- prepare MSG_DATA reply ---------- */
        Message reply = createMessage(
                m.id,
                Message.MSG_DATA,
                nodeId,            // src: me
                m.src,             // dest: requester
                stored.messageTopicID,
                stored.body,       // full payload
                stored.isRow,
                stored.rowOrColumnNumber,
                stored.partNumber,
                CommonState.getTime(),
                m.id               // ack / correlation id
        );

        /* ---------- (optional) keep for further gossips ---------- */
        storeInEphemeralCache(reply);                  // so peers can IHAVE it later

        /* ---------- unicast the data back ---------- */
        publishMessage(reply, m.src, myPid);

        if (isDEBUG) {
            System.out.printf("[DEBUG IWANT] node=%s  → sent MSG_DATA(msgId=%d) to %s%n",
                    nodeId, m.id, m.src);
        }
    }


    public void handleBlockProducerData(Message m, int myPid) {

        Message prev = messageCache.get(m.id);
        if (prev != null && prev.body != null) {
                        if (samePayload(prev.body, m.body)) {
                                duplicateData++;
                            } else {
                                markInvalidMessage(m.src, m.messageTopicID);
                                if (isDEBUG) {
                                        System.out.printf("[MISMATCH] proposer saw conflicting body for msg=%d at t=%d%n",
                                                        m.id, CommonState.getTime());
                                    }
                            }
            return;
        }

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
        sendDataToMesh(m, myPid, m.src);
        samplingStarter();
    }


    // Edger push
    private void sendDataToMesh(Message m, int myPid, BigInteger avoidPeer) {

        /* 1. full body to every mesh peer (MSG_DATA) */
        Message full = createMessage(
                m.id, Message.MSG_DATA,      // <-- DATA, not IHAVE
                nodeId,                  /* src  = me   */
                m.dest,                  /* dest = will be overwritten */
                m.messageTopicID,
                m.body,                  /* full payload */
                m.isRow, m.rowOrColumnNumber, m.partNumber,
                CommonState.getTime(), -6);

        sendMessageToPeers(full, myPid, m.messageTopicID,
                                (avoidPeer == null ? nodeId : avoidPeer),   // skip sender
                                nodeId);

        /* 2. remember it, so the heartbeat can gossip IHAVE later */
        storeInEphemeralCache(full);
    }

    public void handleData(Message m, int myPid) {

        Message prev = messageCache.get(m.id); // have we cached this id?
        if (prev != null && prev.body != null) { // and was it already full data?
                            if (samePayload(prev.body, m.body)) {   // content really identical?
                                    duplicateData++;                    //   → legit duplicate
                                } else {                                // same ID, different bytes!
//                                    markInvalidMessage(m.src, m.messageTopicID);
                                    if (isDEBUG) {
                                            System.out.printf("[MISMATCH] node %s got conflicting body for msg=%d at t=%d%n",
                                                            nodeId, m.id, CommonState.getTime());
                                        }
                                }
            return;
        }

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

            //edger push
            sendDataToMesh(m, myPid, m.src);
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

    /*
     * return true when a request is really queued
     * Builds one sampling round.
     * Returns true if at least one IWANT was queued.
     */
    private boolean sampleDataRequest() {

        final int PARALLEL_ASK = 1; // how many custodians we query at once
        Random rng = CommonState.r;

        boolean isRow = rng.nextBoolean();
        int shardIndex = rng.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512));

        // int rowsColsPerTopic =
        // Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
        // int topicNo = (rowsColsPerTopic == 1)
        // ? (shardIndex * 2) + (isRow ? 1 : 2)
        // : shardIndex / (rowsColsPerTopic / 2);

        int rowsPerTopic = MAX_DIMENSION_SIZE / NUMBER_OF_TOPICS;
        int divisor = (rowsPerTopic == 0) ? 1 : rowsPerTopic;
        int topicNo = (shardIndex / divisor) + 1;

        // int topicNo = (shardIndex / rowsPerTopic) ;

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
        if (holders.isEmpty())
            return false;

        Collections.shuffle(holders, rng);

        int sentNow = 0;
        for (BigInteger dest : holders) {
            if (sentNow == PARALLEL_ASK)
                break;

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
            pendingIWant.add(req.id);
            scheduleRequestTimeout(req, dest);

            sentNow++;
        }
        return sentNow > 0;
    }

    public void startSampling() {
        int sent = 0;
        while (sent < sampleAmount) {
            if (sampleDataRequest())
                sent++; // only count real requests
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
        // EDSimulator.add(SAMPLE_REQ_TIMEOUT, timeout, src, gossipSubId);

    }

    public void handleSampleRequest(Message m, int myPid) {
        // if (isMaliciousNode()) return;

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
        if (pendingIWant.remove(timeoutEvent.msgID)) {
            noOfSampleRequestsSent--;
        }

        if (sentMsg.containsKey(timeoutEvent.msgID)) {

            this.noOfSampleRequestsSent++;
            sampleRequestUnsuccessful++;

            Message sampleMsgSent = sentMsg.get(timeoutEvent.msgID);
            sentMsg.remove(timeoutEvent.msgID);

            BigInteger destId;
            // Random random = new Random();
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
            // EDSimulator.add(SAMPLE_REQ_TIMEOUT, newTimeout, src, gossipSubId);

        }
    }

    // @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;
        recordTickCounters();
        Message m;
        if (!heartbeatScheduled) {
            heartbeatScheduled = true;
            EDSimulator.add(HEARTBEAT_PERIOD, new SimpleEvent(Message.MSG_HEARTBEAT), myNode, myPid);
        }

        switch (((SimpleEvent) event).getType()) {
            case Message.MSG_IHAVE:
                m = (Message) event;
                handleIHave(m, myPid);
                iHaveSent++;
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

                heartbeatManager.runHeartbeat(myPid);
                if (isDEBUG) {
                    for (Map.Entry<String, Set<BigInteger>> e : meshPeersByTopic.entrySet()) {
                        int size = e.getValue().size();

                        if (size != D) {
                            System.out.printf("[MESH‑CHECK] t=%d  node=%s  topic=%s  meshSize=%d  (target=%d)%n",
                                    CommonState.getTime(), nodeId, e.getKey(), size, D);
                        }
                    }
                }

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
            int halfRows = Configuration.getInt("NUMBER_OF_ROWS") / 2;
            // int halfRows = Configuration.getInt("NUMBER_OF_ROWS");
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

        // if (isDEBUG){
        System.out.println("I am holding "
                + (isRow ? "row" : "column")
                + " " + rowOrColumnIndex
                + " for " + destinationId);
        // }

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
            // if (isDEBUG){
            System.out.println();
            System.out.println();
            System.out.println("****");
            System.out.println(topicEntry.getKey());

            // }

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

    // one list of holders per row / column
    private static final List<List<BigInteger>> ROW_HOLDERS = new ArrayList<>();
    private static final List<List<BigInteger>> COL_HOLDERS = new ArrayList<>();
    static {
        for (int i = 0; i < MAX_DIMENSION_SIZE; i++) {
            ROW_HOLDERS.add(new ArrayList<>());
            COL_HOLDERS.add(new ArrayList<>());
        }
    }
    private static final int NUMBER_OF_TOPICS = Configuration.getInt("NUMBER_OF_TOPICS", 1024);
    private static final int ROWS_PER_TOPIC = MAX_DIMENSION_SIZE / NUMBER_OF_TOPICS;
    private static final int COLS_PER_TOPIC = ROWS_PER_TOPIC;

    // static {
    // if (MAX_DIMENSION_SIZE % NUMBER_OF_TOPICS != 0)
    // throw new IllegalArgumentException(
    // "NUMBER_OF_TOPICS (" + NUMBER_OF_TOPICS + ") must divide "
    // + MAX_DIMENSION_SIZE + " exactly.");
    // }

    private static void rememberCustodian(boolean isRow, int index, BigInteger nodeId) {
        List<BigInteger> list = isRow ? ROW_HOLDERS.get(index) : COL_HOLDERS.get(index);
        if (!list.contains(nodeId))
            list.add(nodeId);
    }

    /**
     * Shard the current block and publish:
     * • each row/column is split in <divisions> parts
     * • every part is replicated <SHARD_COPIES> times
     * • topics can be “row first, column second” or vice-versa,
     * controlled by NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC (2 ⇒ 1+1, 4 ⇒ 2+2)
     */
//    private void shardingBasedDistribution() {
//
//        /* ── constants from config ───────────────────────────────────────── */
//        final int divisions = NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC; // shards per row/col
//        final int kCopies = Math.max(1, SHARD_COPIES); // replicas ≥ 1
//        final int rcPerTopic = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 2);
//        // keep symmetric
//        boolean nextTopicIsRow = true;
//
//        /* ── global indices ─────────────────────────────────────────────── */
//        int globalRow = 0;
//        int globalCol = 0;
//
//        Block blk = block;
//
//        /* ── iterate over every topic ────────────────────────────────────── */
//        for (Topic topic : CustomDistribution.topics.values()) {
//
//            int rowsPerTopic, colsPerTopic;
//
//            int rcCfg = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 1);
//
//            if (rcCfg == 1) { // ← new “1 row **or** 1 col” mode
//                rowsPerTopic = nextTopicIsRow ? 1 : 0;
//                colsPerTopic = nextTopicIsRow ? 0 : 1;
//                nextTopicIsRow = !nextTopicIsRow; // flip for the next topic
//            } else { // 2, 4, …
//                rowsPerTopic = rcCfg / 2; // 1+1, 2+2, …
//                colsPerTopic = rowsPerTopic;
//            }
//            System.out.println("----------------------------------------");
//            // System.out.println("Topic-" + topics.values());
//            System.out.printf("[SHARD-DIST] divisions=%d  k=%d  rows/cols per %s = %d/%d%n",
//                    divisions, kCopies, topic.topicID, rowsPerTopic, colsPerTopic);
//
//            List<Node> members = topic.topicMembers;
//            if (members.isEmpty()) {
//                /* give an empty topic a fallback recipient so counters advance */
//                members = Collections.singletonList(CustomDistribution.blockProposerNode);
//            }
//
//            int rowsSentInTopic = 0;
//            int colsSentInTopic = 0;
//            boolean sendRowNext = true; // start with a row
//
//            /* keep sending until the topic got its quota of rows + cols */
//            while (rowsSentInTopic < rowsPerTopic || colsSentInTopic < colsPerTopic) {
//
//                boolean sendingRow = sendRowNext ? rowsSentInTopic < rowsPerTopic
//                        : colsSentInTopic >= colsPerTopic;
//
//                int shardIndex = sendingRow ? globalRow : globalCol;
//                int dimLen = sendingRow
//                        ? Configuration.getInt("NUMBER_OF_ROWS")
//                        : Configuration.getInt("NUMBER_OF_COLUMNS");
//                int partSize = dimLen / divisions;
//
//                if (shardIndex >= MAX_DIMENSION_SIZE)
//                    break; // matrix exhausted
//
//                /* ---- (divisions × k) sends for this one row/col ---- */
//                int sendsNeeded = divisions * kCopies;
//                int cursor = 0; // walks through validators
//
//                for (int send = 0; send < sendsNeeded; send++) {
//
//                    Node peer = members.get(cursor % members.size());
//                    cursor++; // round-robin
//                    GossipSubProtocol dst = (GossipSubProtocol) peer.getProtocol(gossipSubId);
//
//                    int partIdx = send / kCopies; // 0 … divisions-1
//                    int replicaIdx = send % kCopies; // 0 … k-1
//                    int start = partIdx * partSize;
//                    int end = start + partSize;
//
//                    byte[][] slice = sendingRow
//                            ? Arrays.copyOfRange(blk.getRowData(shardIndex), start, end)
//                            : Arrays.copyOfRange(blk.getColumnData(shardIndex), start, end);
//
//                    Message msg = createMessage(
//                            -1, MESSAGE_TYPE,
//                            this.nodeId, dst.nodeId,
//                            topic.topicID,
//                            slice,
//                            sendingRow, // isRow?
//                            shardIndex,
//                            partIdx,
//                            -1, -1);
//
//                    publishMessage(msg, dst.nodeId, gossipSubId);
//
//                    rememberCustodian(sendingRow, shardIndex, dst.nodeId);
//
//                    // if (isDEBUG) {
//                    System.out.printf("[SHARD-DIST] %-3s %-3d  part=%02d copy=%d/%d → %s%n",
//                            sendingRow ? "row" : "col",
//                            shardIndex, partIdx, replicaIdx, kCopies, dst.nodeId);
//                    // }
//                }
//
//                /* ---- bump global & per-topic counters exactly once ---- */
//                if (sendingRow) {
//                    globalRow++;
//                    rowsSentInTopic++;
//                } else {
//                    globalCol++;
//                    colsSentInTopic++;
//                }
//
//                /* alternate rows ↔ cols only when the cfg says “1+1” or “2+2” */
//                if (rowsPerTopic == colsPerTopic)
//                    sendRowNext = !sendRowNext;
//            }
//        }
//
//        /* ── final sanity log ────────────────────────────────────────────── */
//        System.out.printf("Finished: rows=%d  cols=%d%n", globalRow - 1, globalCol - 1);
//        System.out.println("*********Block proposer has sent the messages ********");
//        System.out.printf("Data sent size       : %d%n", totalDataTransmitted);
//        System.out.printf("Data transmission time: %d ms%n", totalTransmissionTime);
//        System.out.println("Malicious Rate: " + Configuration.getDouble("MALICIOUS_RATE"));
//        System.out.println("Seed Number: " + Configuration.getInt("random.seed"));
//        System.out.println("ROW/COL Holder: " + NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC);
//    }

    /**
     * Shard the current block and publish every shard to a validator:
     *   • each row / column is split into <divisions> parts
     *   • every part is replicated <kCopies> times
     *   • the rows / columns assigned to a topic are controlled by
     *     NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC (1 ⇒ row XOR col, 2 ⇒ 1+1, 4 ⇒ 2+2, …)
     *
     * A *fresh shuffle* of the topic-member list is performed for
     * **every single row or column**, so no two labels are forced to
     * share the same contiguous slice of validators.
     */
    private void shardingBasedDistribution() {

        /* ── “per-row/col” constants ─────────────────────────────────── */
        final int divisions = NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC;   // shards per row/col
        final int kCopies   = Math.max(1, SHARD_COPIES);                   // replicas ≥ 1
        final int rcCfg     = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 2);

        boolean nextTopicIsRow = true;   // only used when rcCfg == 1

        /* ── global label counters ───────────────────────────────────── */
        int globalRow = 0;
        int globalCol = 0;

        Block blk = block;               // current block’s matrix data

        /* ── iterate over every topic in order ───────────────────────── */
        for (Topic topic : CustomDistribution.topics.values()) {

            /* how many rows / cols should this topic receive? */
            int rowsPerTopic, colsPerTopic;
            if (rcCfg == 1) {            // “row XOR col” mode
                rowsPerTopic = nextTopicIsRow ? 1 : 0;
                colsPerTopic = nextTopicIsRow ? 0 : 1;
                nextTopicIsRow = !nextTopicIsRow;        // flip for next topic
            } else {                     // symmetric modes: 1+1, 2+2, …
                rowsPerTopic = rcCfg / 2;
                colsPerTopic = rowsPerTopic;
            }

            System.out.printf(
                    "[SHARD-DIST] divisions=%d  k=%d  rows/cols per %s = %d/%d%n",
                    divisions, kCopies, topic.topicID, rowsPerTopic, colsPerTopic);

            /* ensure we have at least one recipient so counters advance */
            List<Node> members = topic.topicMembers.isEmpty()
                    ? Collections.singletonList(CustomDistribution.blockProposerNode)
                    : topic.topicMembers;

            int rowsSentInTopic = 0;
            int colsSentInTopic = 0;
            boolean sendRowNext = true;  // alternation when rowsPerTopic == colsPerTopic

            /* keep sending until the topic has received its quota */
            while (rowsSentInTopic < rowsPerTopic || colsSentInTopic < colsPerTopic) {

                /* choose whether we are sending a row or a column this turn */
                boolean sendingRow = sendRowNext
                        ? rowsSentInTopic < rowsPerTopic
                        : colsSentInTopic >= colsPerTopic;

                int shardIndex = sendingRow ? globalRow : globalCol;
                int dimLen = sendingRow
                        ? Configuration.getInt("NUMBER_OF_ROWS")
                        : Configuration.getInt("NUMBER_OF_COLUMNS");

                if (shardIndex >= dimLen) break;         // matrix exhausted

                int partSize = dimLen / divisions;

                /* ── one fresh permutation for THIS row/column ───────── */
                List<Node> shuffled = new ArrayList<>(members);
                Collections.shuffle(shuffled, CommonState.r);   // Peersim PRNG

                /* ── (divisions × kCopies) transmissions ────────────── */
                for (int part = 0; part < divisions; part++) {
                    for (int copy = 0; copy < kCopies; copy++) {

                        /* pick without replacement inside this row/col */
                        int idx = part * kCopies + copy;            // 0 … divisions·k-1
                        Node peer = shuffled.get(idx % shuffled.size());

                        GossipSubProtocol dst =
                                (GossipSubProtocol) peer.getProtocol(gossipSubId);

                        int start =  part      * partSize;
                        int end   = (part == divisions - 1)
                                ? dimLen                       // last part may be longer
                                : start + partSize;

                        byte[][] slice = sendingRow
                                ? Arrays.copyOfRange(blk.getRowData(shardIndex),    start, end)
                                : Arrays.copyOfRange(blk.getColumnData(shardIndex), start, end);

                        Message msg = createMessage(
                                -1, MESSAGE_TYPE,
                                this.nodeId, dst.nodeId,
                                topic.topicID,
                                slice,
                                sendingRow,          // isRow?
                                shardIndex,
                                part,
                                copy,
                                -1);                 // extra field unused here

                        publishMessage(msg, dst.nodeId, gossipSubId);
                        rememberCustodian(sendingRow, shardIndex, dst.nodeId);

//                        if (isDEBUG) {
                            System.out.printf(
                                    "[SHARD-DIST] %-3s %-4d part=%02d copy=%d/%d → %s%n",
                                    sendingRow ? "row" : "col",
                                    shardIndex, part, copy + 1, kCopies, dst.nodeId);
//                        }
                    }
                }

                /* ── advance global and per-topic counters ───────────── */
                if (sendingRow) {
                    globalRow++;
                    rowsSentInTopic++;
                } else {
                    globalCol++;
                    colsSentInTopic++;
                }

                /* alternate only when rows == cols per topic */
                if (rowsPerTopic == colsPerTopic) sendRowNext = !sendRowNext;
            }
        }

        /* ── final statistics ────────────────────────────────────────── */
        System.out.printf("Finished: rows=%d  cols=%d%n", globalRow - 1, globalCol - 1);
        System.out.println("********* Block proposer has sent the messages ********");
        System.out.printf("Data sent size        : %d%n", totalDataTransmitted);
        System.out.printf("Data transmission time: %d ms%n", totalTransmissionTime);
        System.out.println("Malicious Rate        : " + Configuration.getDouble("MALICIOUS_RATE"));
        System.out.println("Seed Number           : " + Configuration.getInt("random.seed"));
        System.out.println("ROW/COL Holder        : " + NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC);
    }


}
