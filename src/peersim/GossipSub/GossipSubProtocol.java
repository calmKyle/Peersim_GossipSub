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
    private static final int SHARD_AMOUNT =
            Configuration.getInt("SHARD_AMOUNT",
                    /* fallback: keep old heuristic */
                    (NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1)
                            ? NUMBER_OF_VALIDATORS_PER_TOPIC
                            : (int)Math.ceil((NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) /
                            Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC")));

    private static final double GOSSIP_FACTOR = Configuration.getDouble("GOSSIP_FACTOR", 0.33);
    private static final int threshold = Configuration.getInt("THRESHOLD", 1);

//    private static final long SAMPLE_REQ_TIMEOUT = Configuration.getLong("SAMPLE_REQ_TIMEOUT", 4000); // 4 s fallback

//    private static final long SLOT_DURATION = Configuration.getLong("SLOT_DURATION", 12000); // 12 s fallback

    public static String prefix;

    // Instance Variables
    public BigInteger nodeId;
    public UnreliableTransport transport;
    public int tid;
    private int gossipSubId;
    public int D_LOW = Configuration.getInt("MIN_DEGREE", 4);
    public int D_HIGH = Configuration.getInt("MAX_DEGREE", 8);
    protected int D = Configuration.getInt("DEGREE", 8);;
    public boolean ALLOW_EXCEED_D_HIGH_ON_DOUT = Configuration.getBoolean("ALLOW_EXCEED_D_HIGH_ON_DOUT", false);

    // ── sharding‐specific tunables
    // ────────────────────────────────────────────────
    private static final int SHARD_COPIES = Configuration.getInt("SHARD_COPIES", 1);

    public static Block block = new Block(
            Configuration.getInt("NUMBER_OF_ROWS"),
            Configuration.getInt("NUMBER_OF_COLUMNS"));

    public Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> meshPeersByTopic = new HashMap<>();
    protected Map<String, Set<BigInteger>> gossipMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> topicNodes = new HashMap<>();

    private LinkedHashMap<Long, Message> messageCache = new LinkedHashMap<>();
    private LinkedHashMap<Long, Message> IWANTmessageCache = new LinkedHashMap<>();

    public IncrementalStats seedArrivalTimeStore = new IncrementalStats();
    public IncrementalStats seedPartArrivalTimeStore = new IncrementalStats();
    public IncrementalStats samplingDelayTimeStore = new IncrementalStats();

    public List<Long> messageArrivalTimeFromBP = new ArrayList<>();
    public List<Long> messageDelayTimeFromBP = new ArrayList<>();
    public List<Long> seedPartArrivalTimeFromPeer = new ArrayList<>();
    public List<Long> seedPartDelayTimeFromPeer = new ArrayList<>();
    public List<Long> sampleArrivalTime = new ArrayList<>();
    public List<Long> sampleDelayTime = new ArrayList<>();

    private List<Message> custodyData1 = new ArrayList<>();
//    private List<Message> custodyData2 = new ArrayList<>();
    private List<Message> dataReceivedFromBP = new ArrayList<>();
    private List<Message> custody1Parts = new ArrayList<>();
//    private List<Message> custody2Parts = new ArrayList<>();

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
//    private final HashSet<Long> seenIHave   = new HashSet<>();
    private final Map<BigInteger, HashSet<Long>> seenIHaveByPeer = new HashMap<>();


    public long duplicateShards = 0;
    private final HashSet<Long> seenShards = new HashSet<>();

    public long uniqueShards  = 0;

    private final Set<Long> pendingIWant = new HashSet<>();

    public String custody1;
//    public String custody2;
    private boolean samplingStarted = false;
    private boolean blockIsReset = false;

    private int sampleAmount = Configuration.getInt("SAMPLE_AMOUNT", 75);

    protected static List<Set<BigInteger>> rowColHolders = new ArrayList<>(1024);

    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

    private Set<Long> seenMessageIDs = new HashSet<>();

    // Opportunistic Graft
    private final Map<String, Set<BigInteger>> outboundByTopic = new HashMap<>();
    private final Map<String, Set<BigInteger>> inboundByTopic  = new HashMap<>();

    private long lastOpportunisticCheckMs = 0L;
    private final int D_OUT;
    private static final long OP_GRAFT_INTERVAL_MS = 60_000L;
    private static final int  OP_GRAFT_K = 2;
    private static final double OP_GRAFT_MEDIAN_THRESHOLD = 0;

    private static final boolean ADAPTIVE_SAMPLING =
            Configuration.getBoolean("ADAPTIVE_SAMPLING", false);

    private static final int PARALLEL_ASK_HIGH =
            Configuration.getInt(
                    "PARALLEL_ASK_HIGH",
                    Configuration.getInt("PARALLEL_ASK", 1)
            );
    private static int PARALLEL_ASK = Configuration.getInt("PARALLEL_ASK", 1); // how many custodians we query at once



    private void markOutbound(String topicId, BigInteger peer) {
        outboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>()).add(peer);
        inboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>()); // đảm bảo map tồn tại
    }

    private void markInbound(String topicId, BigInteger peer) {
        inboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>()).add(peer);
        outboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
    }

    private void unmarkDirections(String topicId, BigInteger peer) {
        Set<BigInteger> out = outboundByTopic.get(topicId);
        if (out != null) out.remove(peer);
        Set<BigInteger> in  = inboundByTopic.get(topicId);
        if (in  != null) in.remove(peer);
    }

    private int outboundCount(String topicId) {
        return outboundByTopic.getOrDefault(topicId, Collections.emptySet()).size();
    }

    private boolean isInOutbound(String topicId, BigInteger peer) {
        return outboundByTopic.getOrDefault(topicId, Collections.emptySet()).contains(peer);
    }

    private boolean isInInbound(String topicId, BigInteger peer) {
        return inboundByTopic.getOrDefault(topicId, Collections.emptySet()).contains(peer);
    }

   // heartbeat
    private HeartbeatManager heartbeatManager;

    /****************************************************************/
    // Normal nodes flag
    private boolean isBlockProposerNode = false;
    private boolean isValidatorNode = true;

    // Malicious nodes flags
    private boolean isOmissionNode = false; // Ommision
    private boolean isFloodingNode  = false; // Flooding

    public boolean isOmissionNode() {
        return isOmissionNode;
    }

    public void setOmissionNode(boolean maliciousNode) {
        isOmissionNode = maliciousNode;
    }

    public boolean isFloodingNode() {
        return isFloodingNode;
    }

    public void setFloodingNode(boolean maliciousNode) {
        isFloodingNode = maliciousNode;
    }

    private Message buildFloodMsg(String topicID) {
        int sz = Configuration.getInt("FLOOD_PAYLOAD", 256);
        byte[] payload = new byte[sz];
        CommonState.r.nextBytes(payload);

        return createMessage(
                -1,                      // let Message assign a fresh id
                Message.MSG_DATA,
                nodeId,                  // src = me
                null,                    // dest will be set in sendDataToMesh
                topicID,
                payload,                 // bogus body
                false, -1, -1,           // row/col parts unused here
                CommonState.getTime(), -1);
    }


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

        //D_out ~= min(D_LOW - 1, D/2), guarentee ≥ 1
        int candidate = Math.min(Math.max(1, D / 2), Math.max(1, D_LOW - 1));
        this.D_OUT = Math.max(1, candidate);

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


//    public double computeScore(PeerScoreInfo psi) {
//        long nowMs = CommonState.getTime();
//        double topicSum = 0.0;
//
//        // Only allocate the builder if we will actually log
//        final boolean traceOn = true;
//        StringBuilder trace = null;
//        if (traceOn) {
//            trace = new StringBuilder(512);
//            trace.append("│  computing score for peer ")
//                    .append(psi.nodeId)
//                    .append(" @ ").append(nowMs).append('\n');
//        }
//
//
//        /* ---------- P1-P4 per-topic ---------- */
//        for (Map.Entry<String, PeerScoreInfo.TopicScores> e : psi.getTopicsView().entrySet()) {
//            String topic = e.getKey();
//            PeerScoreInfo.TopicScores ts = e.getValue();
//            GossipScoringConfig.TopicParam p =
//                    TOPIC_PARAMS.getOrDefault(topic, GossipScoringConfig.DEFAULT_TOPIC_PARAM);
//
//            double secsInTopic = (ts.timeInMeshStart < 0)
//                    ? 0.0
//                    : Math.max(0.0, (nowMs - ts.timeInMeshStart) / 1000.0);
//            double P1_time = p.timeInMeshWeight *
//                    Math.min(secsInTopic, p.timeInMeshCapSeconds);
//
//            double P2_first = p.firstMsgWeight *
//                    Math.min(ts.firstMessageDeliveries, p.firstMsgCap);
//
//            boolean p3Active = secsInTopic >= p.meshDeliveriesActivationSeconds;
//            double P3_mesh = 0.0;
//            if (p3Active) {
//                double delivered = Math.min(ts.meshMsgDelivered, p.meshDeliveriesCap);
//                double expected  = ts.meshMsgExpected;
//                double diff      = delivered - expected;
//
//                if (diff < 0) {
//                    P3_mesh = p.meshDeliveriesWeightUnder * Math.min(-diff, p.meshDeliveriesCap);
//                } else {
//                    P3_mesh = p.meshDeliveriesWeightOver  * Math.min( diff, p.meshDeliveriesCap);
//                }
//            }
//
//
//            double P4_invalid = p.invalidMsgWeight * ts.invalidMessages;
//
//            double subtotal = p.topicWeight * (P1_time + P2_first + P3_mesh + P4_invalid);
//            topicSum += subtotal;
//
//
//            if (traceOn) {
//                trace.append("│  topic ").append(topic).append("  P1=")
//                        .append(P1_time).append("  P2=").append(P2_first)
//                        .append("  P3=").append(P3_mesh).append("  P4=")
//                        .append(P4_invalid).append("  w=")
//                        .append(p.topicWeight).append("  → subtotal=")
//                        .append(subtotal).append('\n');
//            }
//        }
//
//        /* ---------- TopicCap (TC) ---------- */
//        double total = Math.min(topicSum, GossipScoringConfig.TOPIC_CAP);
//        if (traceOn) {
//            trace.append("│  Σ topics = ").append(topicSum)
//                    .append(" → after TC(").append(GossipScoringConfig.TOPIC_CAP)
//                    .append(") = ").append(total).append('\n');
//        }
//
//        /* ---------- P5 / P6 ---------- */
//        double P5_app  = GossipScoringConfig.P5_WEIGHT * psi.appScore;
//        double P6_ip   = GossipScoringConfig.P6_WEIGHT * psi.ipSurplus;
//        total += P5_app + P6_ip;
//
//
//        if (traceOn) {
//            trace.append("│  P5_app=").append(P5_app)
//                    .append("  P6_ip=").append(P6_ip)
//                    .append("  → TOTAL = ").append(total).append('\n')
//                    .append("└──────────────────────────────────────\n");
//            System.out.println(trace.toString());
//        }
//
//
//        psi.cachedScore = total;
//        return total;
//    }

    public double computeScore(PeerScoreInfo psi) {
        long nowMs = CommonState.getTime();
        double topicSum = 0.0;

        final boolean traceOn = isDEBUG;
        StringBuilder trace = null;
        if (traceOn) {
            trace = new StringBuilder(512);
            trace.append("│  computing score for peer ")
                    .append(psi.nodeId)
                    .append(" @ ").append(nowMs).append('\n');
        }

        for (Map.Entry<String, PeerScoreInfo.TopicScores> e : psi.getTopicsView().entrySet()) {
            String topic = e.getKey();
            PeerScoreInfo.TopicScores ts = e.getValue();
            GossipScoringConfig.TopicParam p =
                    TOPIC_PARAMS.getOrDefault(topic, GossipScoringConfig.DEFAULT_TOPIC_PARAM);

//            double secsInTopic = (ts.timeInMeshStart < 0)
//                    ? 0.0
//                    : Math.max(0.0, (nowMs - ts.timeInMeshStart) / 1000.0);
//            double P1_time = p.timeInMeshWeight *
//                    Math.min(secsInTopic, p.timeInMeshCapSeconds);

            double secsInTopic = (nowMs - ts.timeInMeshStart) / 1000.0;
            double P1_time = p.timeInMeshWeight * Math.min(secsInTopic, p.timeInMeshCapSeconds);


            double P2_first = p.firstMsgWeight *
                    Math.min(ts.firstMessageDeliveries, p.firstMsgCap);

            boolean p3Active =
                    ts.timeInMeshStart >= 0 &&
                            (secsInTopic >= p.meshDeliveriesActivationSeconds || ts.meshMsgDelivered > 0);

//            boolean samplingActive = (noOfSamplesReceived < sampleAmount);
//            boolean p3Active =
//                    ts.timeInMeshStart >= 0 &&
//                            (secsInTopic >= p.meshDeliveriesActivationSeconds || ts.meshMsgDelivered > 0) &&
//                            samplingActive;

            double P3_mesh = 0.0;
            if (p3Active) {
                double delivered = Math.min(ts.meshMsgDelivered, p.meshDeliveriesCap);
//                double expected  = ts.meshMsgExpected;
                double expected = p.meshDeliveriesThreshold;
                double diff      = delivered - expected;

                if (diff < 0) {
                    P3_mesh = p.meshDeliveriesWeightUnder * Math.min(-diff, p.meshDeliveriesCap);
                } else {
                    P3_mesh = p.meshDeliveriesWeightOver  * Math.min( diff, p.meshDeliveriesCap);
                }
            }

            double P4_invalid = p.invalidMsgWeight * ts.invalidMessages;

            double subtotal = p.topicWeight * (P1_time + P2_first + P3_mesh + P4_invalid);
            topicSum += subtotal;

            if (traceOn) {
                trace.append("│  topic ").append(topic).append("  P1=")
                        .append(P1_time).append("  P2=").append(P2_first)
                        .append("  P3=").append(P3_mesh).append("  P4=")
                        .append(P4_invalid).append("  w=")
                        .append(p.topicWeight).append("  → subtotal=")
                        .append(subtotal).append('\n');
            }
        }

        double total = Math.min(topicSum, GossipScoringConfig.TOPIC_CAP);
        if (traceOn) {
            trace.append("│  Σ topics = ").append(topicSum)
                    .append(" → after TC(").append(GossipScoringConfig.TOPIC_CAP)
                    .append(") = ").append(total).append('\n');
        }

        double P5_app  = GossipScoringConfig.P5_WEIGHT * psi.appScore;
        double P6_ip   = GossipScoringConfig.P6_WEIGHT * psi.ipSurplus;
        total += P5_app + P6_ip;

        if (traceOn) {
//            trace.append("│  P5_app=").append(P5_app)
//                    .append("  P6_ip=").append(P6_ip)
//                    .append("  → TOTAL = ").append(total).append('\n')
//                    .append("└──────────────────────────────────────\n");
            System.out.println(trace.toString());
        }

        psi.cachedScore = total;
        return total;
    }




    private double score(BigInteger p) {
        GossipTracer gt = gossipTracer.get(p);
        if (gt == null || gt.ihaveSeen == 0) return 1.0;   // no history → be fair
        return (double) gt.iwantSent / gt.ihaveSeen;       // 0 … 1
    }

    void advertiseMessageIHAVE(Message originalMsg, int myPid) {
        Set<BigInteger> all = topicNodes.getOrDefault(originalMsg.messageTopicID, Collections.emptySet());
        Set<BigInteger> inMesh = meshPeersByTopic.getOrDefault(originalMsg.messageTopicID, Collections.emptySet());

        List<BigInteger> candidates = new ArrayList<>();
        for (BigInteger p : all) {
            if (!inMesh.contains(p) && !p.equals(nodeId)) candidates.add(p);
        }
        if (candidates.isEmpty()) candidates.addAll(inMesh);

        if (candidates.isEmpty()) return;

        int advertiseCnt = USE_ADAPTIVE_GOSSIP
                ? Math.min(D_LAZY, candidates.size())
                : Math.max(1, (int) Math.ceil(GOSSIP_FACTOR * candidates.size()));

        Collections.shuffle(candidates, CommonState.r);
        Message advert = createMessage(originalMsg.id, Message.MSG_IHAVE, nodeId, null,
                originalMsg.messageTopicID, null, originalMsg.isRow, originalMsg.rowOrColumnNumber,
                originalMsg.partNumber, CommonState.getTime(), -6);

        for (int i = 0; i < advertiseCnt; i++) {
            BigInteger peer = candidates.get(i);
            if (peer.equals(nodeId)) continue;
            Message copy = (Message) advert.copy();
            copy.dest = peer;
            publishMessage(copy, peer, myPid);
        }
    }


//    public void advertiseMessageIHAVE(Message originalMsg, int myPid) {
//        Set<BigInteger> peers = meshPeersByTopic.getOrDefault(originalMsg.messageTopicID, Collections.emptySet());
//        if (peers.isEmpty())
//            return;
//
////        int advertiseCnt = Math.max(1, (int) Math.ceil(GOSSIP_FACTOR * peers.size()));
//        int advertiseCnt = USE_ADAPTIVE_GOSSIP
//                ? Math.min(D_LAZY, peers.size())
//                : Math.max(1, (int) Math.ceil(GOSSIP_FACTOR * peers.size()));
////        List<BigInteger> shuffled = new ArrayList<>(peers);
//        List<BigInteger> picks = new ArrayList<>(peers);
//
//        if (USE_ADAPTIVE_GOSSIP) {
//            picks.sort((p,q) ->          /* descending by “usefulness” ratio */
//                    Double.compare(score(q), score(p)));
//        }
//
//        Collections.shuffle(picks.subList(
//                        USE_ADAPTIVE_GOSSIP ? Math.min(peers.size(), 4) : 0, picks.size()),
//                CommonState.r);
////        Collections.shuffle(shuffled, CommonState.r);
//
//        Message advert = createMessage(originalMsg.id, Message.MSG_IHAVE, nodeId, null,
//                originalMsg.messageTopicID, null,
//                originalMsg.isRow, originalMsg.rowOrColumnNumber,
//                originalMsg.partNumber, CommonState.getTime(), -6);
//
//        for (int i = 0; i < advertiseCnt; i++) {
//            BigInteger peer = picks.get(i);
//            if (peer.equals(nodeId))
//                continue;
//            Message copy = (Message) advert.copy();
//            copy.dest = peer;
//            publishMessage(copy, peer, myPid);
//        }
//    }

    private List<BigInteger> selectHighScorePeersForTopic(
            String topicId,
            Set<BigInteger> disallow, // ví dụ: mesh ∪ backoff ∪ {self} ∪ {peer đang xử lý}
            int limit) {

        Set<BigInteger> all = topicNodes.getOrDefault(topicId, Collections.emptySet());
        List<BigInteger> cands = new ArrayList<>(Math.max(16, limit * 2));

        for (BigInteger p : all) {
            if (p.equals(nodeId)) continue;
            if (disallow != null && disallow.contains(p)) continue;

            addPeerScoreIfAbsent(p);
            PeerScoreInfo psi = peerScores.get(p);

            if (psi != null && psi.isPruneBackoffActiveForTopic(topicId, CommonState.getTime())) {
                continue;
            }
            cands.add(p);
        }


        long now = CommonState.getTime();

        cands.sort((a, b) -> {
            double sb = computeScore(peerScores.get(b));
            double sa = computeScore(peerScores.get(a));
            return Double.compare(sb, sa);
        });

        if (cands.size() > limit) {
            return new ArrayList<>(cands.subList(0, limit));
        }
        return cands;
    }


//    public void updateMeshConnections() {
//        if (subscribedTopics.isEmpty())
//            return;
//
//        for (String topicID : subscribedTopics.stream().map(t -> t.topicID).collect(Collectors.toList())) {
//            meshPeersByTopic.putIfAbsent(topicID, new HashSet<>());
//            Set<BigInteger> peers = meshPeersByTopic.get(topicID);
//            if (peers == null)
//                continue;
//
//            if (peers.size() < D_LOW) {
//                addMorePeers(topicID, D - peers.size());
//            }
//            if (peers.size() > D_HIGH) {
//                removeExcessPeers(topicID, peers.size() - D);
//            }
//        }
//    }

//    private void updateMeshConnections(String topicId, int pid) {
//        Set<BigInteger> mesh = meshPeersByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
//        int size = mesh.size();
//
//        if (size < D_LOW) {
//            addMorePeers(topicId, D - size, pid);
//        } else if (size > D_HIGH) {
//            removeExcessPeers(topicId, size - D, pid);
//        }
//
//        int out = outboundCount(topicId);
//        if (out >= D_OUT) return; // enough outbound
//
//        final long now = CommonState.getTime();
//        int toAdd = D_OUT - out;
//
//        java.util.function.Predicate<BigInteger> isInBackoff = (peer) -> {
//            PeerScoreInfo psi = peerScores.get(peer);
//            if (psi == null) return false;
//            PeerScoreInfo.TopicScores ts =
//                    (psi.topicScoresMap != null) ? psi.topicScoresMap.get(topicId) : null;
//            if (ts != null && ts.pruneBackoffUntil > now) return true;
//            return psi.pruneBackoffUntil > now; // fallback
//        };
//
//        Set<BigInteger> outbound = outboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
//        Set<BigInteger> inbound  = inboundByTopic.computeIfAbsent(topicId,  k -> new HashSet<>());
//
//        // ──────────────────────────────────────────────────────────
//        List<BigInteger> inboundOnly = new ArrayList<>();
//        for (BigInteger p : mesh) {
//            if (!outbound.contains(p) && inbound.contains(p) && !isInBackoff.test(p)) {
//                inboundOnly.add(p);
//            }
//        }
//        inboundOnly.sort((a, b) -> {
//            double sb = computeScore(peerScores.getOrDefault(b, new PeerScoreInfo(now)));
//            double sa = computeScore(peerScores.getOrDefault(a, new PeerScoreInfo(now)));
//            return Double.compare(sb, sa);
//        });
//
//        for (BigInteger p : inboundOnly) {
//            if (toAdd <= 0) break;
//            addPeerScoreIfAbsent(p);
//            Message graft = createMessage(/* id */ -1, Message.MSG_GRAFT, nodeId, p,
//                    topicId, null, false, -1, -1, now, /*via*/ -5);
//            publishMessage(graft, p, pid);
//            markOutbound(topicId, p);
//            toAdd--;
//        }
//        if (toAdd <= 0) return;
//
//        // ──────────────────────────────────────────────────────────
//        boolean allowExceed = false;
//        try { allowExceed = ALLOW_EXCEED_D_HIGH_ON_DOUT; } catch (Throwable ignore) {}
//
//        Set<BigInteger> disallow = new HashSet<>();
//        disallow.add(nodeId);
//
//        List<BigInteger> candidates = selectHighScorePeersForTopic(topicId, disallow, toAdd * 3);
//
//        List<BigInteger> outsideMesh = new ArrayList<>();
//        List<BigInteger> inMeshButNotOutbound = new ArrayList<>();
//        for (BigInteger p : candidates) {
//            if (p.equals(nodeId)) continue;
//            if (outbound.contains(p)) continue;
//            if (isInBackoff.test(p)) continue;
//
//            if (!mesh.contains(p)) outsideMesh.add(p);
//            else if (!outbound.contains(p) && inbound.contains(p)) inMeshButNotOutbound.add(p);
//        }
//
//        java.util.function.Supplier<BigInteger> worstInboundSupplier = () -> {
//            BigInteger worstInbound = null;
//            double worstScore = Double.POSITIVE_INFINITY;
//            for (BigInteger q : mesh) {
//                if (inbound.contains(q) && !outbound.contains(q)) {
//                    double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(now)));
//                    if (s < worstScore) { worstScore = s; worstInbound = q; }
//                }
//            }
//            return worstInbound;
//        };
//
//        for (BigInteger p : outsideMesh) {
//            if (toAdd <= 0) break;
//
//            if (!allowExceed && mesh.size() >= D_HIGH) {
//                BigInteger worstInbound = worstInboundSupplier.get();
//                if (worstInbound != null) {
//                    prunePeer(worstInbound, topicId);
//                    mesh.remove(worstInbound);
//                    unmarkDirections(topicId, worstInbound);
//                } else {
//                    continue;
//                }
//            }
//
//            mesh.add(p);
//            addPeerScoreIfAbsent(p);
//            Message graft = createMessage(/* id */ -1, Message.MSG_GRAFT, nodeId, p,
//                    topicId, null, false, -1, -1, now, /*via*/ -5);
//            publishMessage(graft, p, pid);
//            markOutbound(topicId, p);
//            toAdd--;
//        }
//        if (toAdd <= 0) return;
//
//        for (BigInteger p : inMeshButNotOutbound) {
//            if (toAdd <= 0) break;
//
//            if (!allowExceed && mesh.size() >= D_HIGH) {
//                BigInteger worstInbound = null;
//                double worstScore = Double.POSITIVE_INFINITY;
//                for (BigInteger q : mesh) {
//                    if (inbound.contains(q) && !outbound.contains(q) && !q.equals(p)) {
//                        double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(now)));
//                        if (s < worstScore) { worstScore = s; worstInbound = q; }
//                    }
//                }
//                if (worstInbound != null) {
//                    prunePeer(worstInbound, topicId);
//                    mesh.remove(worstInbound);
//                    unmarkDirections(topicId, worstInbound);
//                } else {
//                    continue;
//                }
//            }
//
//            addPeerScoreIfAbsent(p);
//            Message graft = createMessage(/* id */ -1, Message.MSG_GRAFT, nodeId, p,
//                    topicId, null, false, -1, -1, now, /*via*/ -5);
//            publishMessage(graft, p, pid);
//            markOutbound(topicId, p);
//            toAdd--;
//
//            if (mesh.size() < D) {
//                int missing = D - mesh.size();
//                addMorePeers(topicId, missing, pid);
//            }
//        }
//    }

    // Prefer evicting outbound-only first (keeps in-degree high → more duplicates)
    private BigInteger pickVictimAtHigh(
            Set<BigInteger> mesh,
            Set<BigInteger> inbound,
            Set<BigInteger> outbound,
            String topicId,
            long now
    ) {
        BigInteger best = null;
        double bestScore = Double.POSITIVE_INFINITY;
        for (BigInteger p : mesh) addPeerScoreIfAbsent(p);

        // type order: 0 = outbound-only, 1 = both, 2 = inbound-only
        java.util.function.BiPredicate<BigInteger, Integer> isType = (q, type) -> {
            boolean in  = inbound.contains(q);
            boolean out = outbound.contains(q);
            if (type == 0) return out && !in;     // outbound-only (first to go)
            if (type == 1) return out && in;      // bidirectional (second)
            return in && !out;                     // inbound-only (last resort)
        };

        for (int type = 0; type < 3; type++) {
            best = null; bestScore = Double.POSITIVE_INFINITY;
            for (BigInteger q : mesh) {
                if (!isType.test(q, type)) continue;
//                double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(q)));
                double s = computeScore(peerScores.get(q));
                if (s < bestScore) { bestScore = s; best = q; }
            }
            if (best != null) return best;
        }
        return null;
    }


    private void updateMeshConnections(String topicId, int pid) {
        Set<BigInteger> mesh = meshPeersByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
        int size = mesh.size();

        // (1) Rebalance to [D_LOW, D_HIGH]
        if (size < D_LOW) {
            addMorePeers(topicId, D - size, pid);
        } else if (size > D_HIGH) {
            removeExcessPeers(topicId, size - D, pid);
        }

        final long now = CommonState.getTime();

        // Backoff checker (per-topic then peer-wide)
        java.util.function.Predicate<BigInteger> isInBackoff = (peer) -> {
            PeerScoreInfo psi = peerScores.get(peer);
            if (psi == null) return false;
            PeerScoreInfo.TopicScores ts =
                    (psi.topicScoresMap != null) ? psi.topicScoresMap.get(topicId) : null;
            if (ts != null && ts.pruneBackoffUntil > now) return true;
            return psi.pruneBackoffUntil > now; // fallback
        };

        Set<BigInteger> outbound = outboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
        Set<BigInteger> inbound  = inboundByTopic.computeIfAbsent(topicId,  k -> new HashSet<>());
        for (BigInteger p : mesh) addPeerScoreIfAbsent(p);

        // (2) Ensure D_OUT: convert inbound->outbound first
        int out = outboundCount(topicId);
        if (out < D_OUT) {
            int toAdd = D_OUT - out;

            // Prefer inbound-only in mesh, highest score first
            List<BigInteger> inboundOnly = new ArrayList<>();
            for (BigInteger p : mesh) {
                if (!outbound.contains(p) && inbound.contains(p) && !isInBackoff.test(p)) {
                    inboundOnly.add(p);
                }
            }
            inboundOnly.sort((a, b) -> {
                double sb = computeScore(peerScores.get(b));
                double sa = computeScore(peerScores.get(a));
                return Double.compare(sb, sa);
            });

            for (BigInteger p : inboundOnly) {
                if (toAdd <= 0) break;
                addPeerScoreIfAbsent(p);
                Message graft = createMessage(-1, Message.MSG_GRAFT, nodeId, p,
                        topicId, null, false, -1, -1, now, /*via*/ -5);
                publishMessage(graft, p, pid);
                markOutbound(topicId, p);
                toAdd--;
            }

            if (toAdd > 0) {
                boolean allowExceed = false;
                try { allowExceed = ALLOW_EXCEED_D_HIGH_ON_DOUT; } catch (Throwable ignore) {}

                // Don’t disallow mesh: we’ll split into outside/in-mesh below
                Set<BigInteger> disallow = new HashSet<>();
                disallow.add(nodeId);

                List<BigInteger> candidates = selectHighScorePeersForTopic(topicId, disallow, toAdd * 3);

                List<BigInteger> outsideMesh = new ArrayList<>();
                List<BigInteger> inMeshButNotOutbound = new ArrayList<>();
                for (BigInteger p : candidates) {
                    if (p.equals(nodeId)) continue;
                    if (outbound.contains(p)) continue;
                    if (isInBackoff.test(p)) continue;

                    if (!mesh.contains(p)) outsideMesh.add(p);
                    else if (inbound.contains(p)) inMeshButNotOutbound.add(p);
                }

                java.util.function.Supplier<BigInteger> worstInboundSupplier = () -> {
                    BigInteger worstInbound = null;
                    double worstScore = Double.POSITIVE_INFINITY;
                    for (BigInteger q : mesh) {
                        if (inbound.contains(q) && !outbound.contains(q)) {
//                            double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(q)));
                            double s = computeScore(peerScores.get(q));
                            if (s < worstScore) { worstScore = s; worstInbound = q; }
                        }
                    }
                    return worstInbound;
                };

                for (BigInteger p : outsideMesh) {
                    if (toAdd <= 0) break;

                    if (!allowExceed && mesh.size() >= D_HIGH) {
                        BigInteger worstInbound = worstInboundSupplier.get();
                        if (worstInbound != null) {
                            prunePeer(worstInbound, topicId);
                        } else {
                            continue;
                        }
                    }

                    mesh.add(p); // optimistic
                    addPeerScoreIfAbsent(p);
                    Message graft = createMessage(-1, Message.MSG_GRAFT, nodeId, p,
                            topicId, null, false, -1, -1, now, /*via*/ -5);
                    publishMessage(graft, p, pid);
                    markOutbound(topicId, p);
                    toAdd--;
                }

                for (BigInteger p : inMeshButNotOutbound) {
                    if (toAdd <= 0) break;

                    if (!allowExceed && mesh.size() >= D_HIGH) {
                        BigInteger worstInbound = worstInboundSupplier.get();
                        if (worstInbound != null && !worstInbound.equals(p)) {
                            prunePeer(worstInbound, topicId);
                        } else {
                            continue;
                        }
                    }

                    addPeerScoreIfAbsent(p);
                    Message graft = createMessage(-1, Message.MSG_GRAFT, nodeId, p,
                            topicId, null, false, -1, -1, now, /*via*/ -5);
                    publishMessage(graft, p, pid);
                    markOutbound(topicId, p);
                    toAdd--;
                }
            }
        }

        if (mesh.size() < D) {
            int missing = D - mesh.size();
            addMorePeers(topicId, missing, pid);
        }
    }


    public void updateMeshConnections() {
        // Use your existing pid field if present; otherwise replace with a constant or a getter
        final int pid = this.gossipSubId;
        final long now = CommonState.getTime();

        // 1) Maintain mesh per topic: rebalance [D_LOW, D_HIGH] + ensure D_OUT
        for (String topicId : meshPeersByTopic.keySet()) {
            updateMeshConnections(topicId, pid);   // your existing 2-arg method
        }

        // 2) Lazy gossip (IHAVE) — safe no-op if you keep stub
        advertisePendingIHave(pid);

        // 3) Decay counters/scores — safe no-op if you keep stub
        decayPeerTopicCountersIfAny();
        decayScoresIfAny();

        // 4) Opportunistic grafting every ~60s
        if (now - lastOpportunisticCheckMs >= OP_GRAFT_INTERVAL_MS) {
            lastOpportunisticCheckMs = now;
            for (String topicId : meshPeersByTopic.keySet()) {
                maybeOpportunisticGraft(topicId, pid);
            }
        }
    }

    public boolean inMyMesh(String topicID, BigInteger peer) {
        Set<BigInteger> mesh = meshPeersByTopic.get(topicID);
        return mesh != null && mesh.contains(peer);
    }


    private void addMorePeers(String topicId, int need, int pid) {
        if (need <= 0) return;

        final long now = CommonState.getTime();

        Set<BigInteger> mesh     = meshPeersByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
        Set<BigInteger> outbound = outboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
        Set<BigInteger> inbound  = inboundByTopic.computeIfAbsent(topicId,  k -> new HashSet<>());

        for (BigInteger p : mesh) addPeerScoreIfAbsent(p);

        int outNow  = outbound.size();
        int outNeed = Math.max(0, D_OUT - outNow);
        int target  = Math.max(need, outNeed);
        if (target <= 0) return;

        java.util.function.Predicate<BigInteger> isInBackoff = (peer) -> {
            PeerScoreInfo psi = peerScores.get(peer);
            if (psi == null) return false;
            PeerScoreInfo.TopicScores ts =
                    (psi.topicScoresMap != null) ? psi.topicScoresMap.get(topicId) : null;
            if (ts != null && ts.pruneBackoffUntil > now) return true;
            return psi.pruneBackoffUntil > now;
        };

        List<BigInteger> inboundOnly = new ArrayList<>();
        for (BigInteger q : mesh) {
            if (outbound.contains(q)) continue;
            if (!inbound.contains(q)) continue;
            if (isInBackoff.test(q)) continue;
            inboundOnly.add(q);
        }
        inboundOnly.sort((a, b) -> {
            double sb = computeScore(peerScores.get(b));
            double sa = computeScore(peerScores.get(a));
            return Double.compare(sb, sa);
        });

        boolean allowExceed = false;
        try { allowExceed = ALLOW_EXCEED_D_HIGH_ON_DOUT; } catch (Throwable ignore) {}

        int remaining = target;

        for (BigInteger p : inboundOnly) {
            if (remaining <= 0) break;
            if (outbound.contains(p)) continue;

            if (!allowExceed && mesh.size() >= D_HIGH) {
                BigInteger worstInbound = null;
                double worstScore = Double.POSITIVE_INFINITY;

                for (BigInteger q : mesh) {
                    if (inbound.contains(q) && !outbound.contains(q) && !q.equals(p)) {
//                        double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(q)));
                        double s = computeScore(peerScores.get(q));
                        if (s < worstScore) { worstScore = s; worstInbound = q; }
                    }
                }

                if (worstInbound != null) {
                    prunePeer(worstInbound, topicId);
                    mesh.remove(worstInbound);
                    unmarkDirections(topicId, worstInbound);
                } else {
                    continue;
                }
            }

            addPeerScoreIfAbsent(p);
            Message graft = createMessage(-1, Message.MSG_GRAFT, nodeId, p,
                    topicId, null, false, -1, -1, now, -5);
            publishMessage(graft, p, pid);
            markOutbound(topicId, p);
            remaining--;
        }

        if (remaining <= 0) return;

        Set<BigInteger> disallow = new HashSet<>();
        disallow.add(nodeId);

        List<BigInteger> selected = selectHighScorePeersForTopic(topicId, disallow, remaining * 3);

        List<BigInteger> outsideMesh = new ArrayList<>();
        List<BigInteger> inMeshButNotOutbound = new ArrayList<>();

        for (BigInteger p : selected) {
            if (p.equals(nodeId)) continue;
            if (outbound.contains(p)) continue;
            if (isInBackoff.test(p)) continue;

            if (!mesh.contains(p)) outsideMesh.add(p);
            else if (inbound.contains(p) && !outbound.contains(p)) inMeshButNotOutbound.add(p);
        }

        for (BigInteger p : outsideMesh) {
            if (remaining <= 0) break;

//            if (!allowExceed && mesh.size() >= D_HIGH) {
//                BigInteger worstInbound = null;
//                double worstScore = Double.POSITIVE_INFINITY;
//
//                for (BigInteger q : mesh) {
//                    if (inbound.contains(q) && !outbound.contains(q)) {
//                        double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(now)));
//                        if (s < worstScore) { worstScore = s; worstInbound = q; }
//                    }
//                }
//
//                if (worstInbound != null) {
//                    prunePeer(worstInbound, topicId);
//                    mesh.remove(worstInbound);
//                    unmarkDirections(topicId, worstInbound);
//                } else {
//                    continue;
//                }
//            }
            if (!allowExceed && mesh.size() >= D_HIGH) {
                BigInteger victim = pickVictimAtHigh(mesh, inbound, outbound, topicId, now);
                if (victim != null) {
                    prunePeer(victim, topicId);
                    mesh.remove(victim);
                    unmarkDirections(topicId, victim);
                } else {
                    continue;
                }
            }


            mesh.add(p);
            addPeerScoreIfAbsent(p);
            Message graft = createMessage(-1, Message.MSG_GRAFT, nodeId, p,
                    topicId, null, false, -1, -1, now, -5);
            publishMessage(graft, p, pid);
            markOutbound(topicId, p);
            remaining--;
        }

        if (remaining <= 0) return;

        for (BigInteger p : inMeshButNotOutbound) {
            if (remaining <= 0) break;

//            if (!allowExceed && mesh.size() >= D_HIGH) {
//                BigInteger worstInbound = null;
//                double worstScore = Double.POSITIVE_INFINITY;
//
//                for (BigInteger q : mesh) {
//                    if (inbound.contains(q) && !outbound.contains(q) && !q.equals(p)) {
//                        double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(now)));
//                        if (s < worstScore) { worstScore = s; worstInbound = q; }
//                    }
//                }
//
//                if (worstInbound != null) {
//                    prunePeer(worstInbound, topicId);
//                    mesh.remove(worstInbound);
//                    unmarkDirections(topicId, worstInbound);
//                } else {
//                    continue;
//                }
//            }

            if (!allowExceed && mesh.size() >= D_HIGH) {
                BigInteger victim = pickVictimAtHigh(mesh, inbound, outbound, topicId, now);
                if (victim != null) {
                    prunePeer(victim, topicId);
                    mesh.remove(victim);
                    unmarkDirections(topicId, victim);
                } else {
                    continue;
                }
            }


            addPeerScoreIfAbsent(p);
            Message graft = createMessage(-1, Message.MSG_GRAFT, nodeId, p,
                    topicId, null, false, -1, -1, now, -5);
            publishMessage(graft, p, pid);
            markOutbound(topicId, p);
            remaining--;
        }
    }




    public void handleGraft(Message m, int myPid) {
        final String    topicID = m.messageTopicID;
        final BigInteger p      = m.src;
        final long      now     = CommonState.getTime();

        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] Node " + nodeId
                    + " received GRAFT from peer " + p
                    + " topic=" + topicID
                    + " t=" + now);
        }

        addPeerScoreIfAbsent(p);
        PeerScoreInfo psi = peerScores.get(p);
        if (psi == null) {
            if (isDEBUG) {
                System.out.println("[DEBUG handleGraft] No PeerScoreInfo for " + p + "; ignoring.");
            }
            return;
        }

        double s = computeScore(psi);
        long topicBackoffUntil = 0L;
        if (psi.topicScoresMap != null) {
            PeerScoreInfo.TopicScores ts = psi.topicScoresMap.get(topicID);
            if (ts != null) topicBackoffUntil = ts.pruneBackoffUntil;
        }
        boolean inBackoff = (now < topicBackoffUntil) || (now < psi.pruneBackoffUntil);

        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] Peer " + p + " score=" + s
                    + " topicBackoffUntil=" + topicBackoffUntil
                    + " peerBackoffUntil=" + psi.pruneBackoffUntil
                    + " inBackoff=" + inBackoff);
        }

        if (s < 0 || inBackoff) {
            if (isDEBUG) {
                System.out.println("[DEBUG handleGraft] Deny GRAFT from " + p + " (score<0 or backoff); sending PRUNE.");
            }
            prunePeer(p, topicID);
            return;
        }

        Set<BigInteger> mesh = meshPeersByTopic.computeIfAbsent(topicID, k -> new HashSet<>());
        boolean added = mesh.add(p);
        markInbound(topicID, p);

        if (added) {
            // ensure PeerScoreInfo exists
            addPeerScoreIfAbsent(p);
            PeerScoreInfo psi_added = peerScores.get(p);
            PeerScoreInfo.TopicScores ts =
                    psi_added.topicScoresMap.computeIfAbsent(topicID, t -> new PeerScoreInfo.TopicScores());

            ts.timeInMeshStart = now;
        }

        if (isDEBUG) {
            System.out.println("[DEBUG handleGraft] ACCEPT " + p + " into mesh[" + topicID + "]"
                    + " (added=" + added + ") size=" + mesh.size());
        }


        if (mesh.size() > D_HIGH) {
            int over = mesh.size() - D;
            if (isDEBUG) {
                System.out.printf("[DEBUG handleGraft] mesh[%s] oversized %d>%d; prune %d%n",
                        topicID, mesh.size(), D, over);
            }
            removeExcessPeers(topicID, over, myPid);
        }
    }

    public void handlePrune(Message m, int myPid) {
        final BigInteger pruner  = m.src;
        final String     topicID = m.messageTopicID;
        final long       now     = CommonState.getTime();

        if (isDEBUG) {
            System.out.println("[DEBUG handlePrune] Node " + nodeId
                    + " received PRUNE from " + pruner
                    + " topic=" + topicID
                    + " t=" + now);
        }

        Set<BigInteger> mesh = meshPeersByTopic.computeIfAbsent(topicID, k -> new HashSet<>());
        boolean wasPresent = mesh.remove(pruner);

        unmarkDirections(topicID, pruner);

        if (wasPresent) {
            PeerScoreInfo psi = peerScores.get(pruner);
            if (psi != null) {
                PeerScoreInfo.TopicScores ts = psi.topicScoresMap.get(topicID);
                if (ts != null) {
                    // stop accruing P1/P3 for this topic
                    ts.timeInMeshStart = -1L;
                }
            }
        }

        if (!wasPresent) {
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] Peer " + pruner
                        + " not in local mesh[" + topicID + "]; ignore.");
            }
        } else {
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] Removed " + pruner
                        + " from mesh[" + topicID + "]; size=" + mesh.size());
            }
        }

        List<BigInteger> px = m.getPrunePX();
        if (px != null && !px.isEmpty()) {

            Set<BigInteger> all = topicNodes.computeIfAbsent(topicID, k -> new HashSet<>());
            all.addAll(px);
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] PX size=" + px.size() + " added to known set for topic " + topicID);
            }
        }

        addPeerScoreIfAbsent(pruner);
        PeerScoreInfo psi = peerScores.get(pruner);
        if (psi != null) {
            long backoff = 60_000L;
//            PeerScoreInfo.TopicScores ts =
//                    psi.topicScoresMap.computeIfAbsent(topicID, k -> new PeerScoreInfo.TopicScores());
            PeerScoreInfo.TopicScores ts = psi.getOrCreate(topicID);

            ts.pruneBackoffUntil = Math.max(ts.pruneBackoffUntil, now + backoff);
            psi.pruneBackoffUntil = Math.max(psi.pruneBackoffUntil, now + backoff);
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] Set local backoff for pruner " + pruner
                        + " until topic=" + ts.pruneBackoffUntil + " (global=" + psi.pruneBackoffUntil + ")");
            }
        }

        int sizeAfter = mesh.size();
        if (sizeAfter < D_LOW) {
            int needed = D - sizeAfter;
            if (needed > 0) {
                if (isDEBUG) {
                    System.out.println("[DEBUG handlePrune] mesh[" + topicID + "] too small ("
                            + sizeAfter + "<" + D_LOW + "); GRAFT " + needed + " peers...");
                }
                addMorePeers(topicID, needed, myPid);
            }
        } else if (sizeAfter > D_HIGH) {
            int over = sizeAfter - D;
            if (isDEBUG) {
                System.out.println("[DEBUG handlePrune] mesh[" + topicID + "] oversubscribed size="
                        + sizeAfter + " > " + D_HIGH + "; prune " + over + " peers...");
            }
            removeExcessPeers(topicID, over, myPid);
        }


    }

    // e.g. in removeExcessPeers(...) or prunePeer(...)
//    public void prunePeer(BigInteger peerID, String topicID) {
//        long now = CommonState.getTime();
//
//        Set<BigInteger> all = topicNodes.getOrDefault(topicID, Collections.emptySet());
//        Set<BigInteger> meshPeers = meshPeersByTopic.getOrDefault(topicID, new HashSet<>());
//        boolean wasPresent = meshPeers.remove(peerID);
//
//        List<BigInteger> candidates = new ArrayList<>();
//        for (BigInteger q : all) {
//            if (q.equals(peerID)) continue;
//            if (q.equals(nodeId)) continue;
//            if (meshPeers != null && meshPeers.contains(q)) continue;
//            candidates.add(q);
//        }
//
//        if (candidates.isEmpty()) {
//            for (BigInteger q : all) {
//                if (!q.equals(peerID) && !q.equals(nodeId)) {
//                    candidates.add(q);
//                }
//            }
//        }
//
//        candidates.sort((a, b) -> {
//            double sa = computeScore(peerScores.getOrDefault(a, new PeerScoreInfo(now)));
//            double sb = computeScore(peerScores.getOrDefault(b, new PeerScoreInfo(now)));
//            return Double.compare(sb, sa); // descending
//        });
//
//        unmarkDirections(topicID, peerID);
//
//        int PX_LIMIT = 8;
//        List<BigInteger> px = candidates.subList(0, Math.min(PX_LIMIT, candidates.size()));
//
//        if (isDEBUG) {
//            System.out.println("[DEBUG prunePeer] Node " + nodeId
//                    + " is pruning peer " + peerID
//                    + " for topic=" + topicID
//                    + " at time=" + now);
//            if (!wasPresent) {
//                System.out.println("[DEBUG prunePeer] peer " + peerID
//                        + " wasn't in localMesh[" + topicID + "] anyway; ignoring.");
//            } else {
//                System.out.println("[DEBUG prunePeer] localMesh[" + topicID + "] size is now "
//                        + meshPeers.size() + " after removing " + peerID);
//            }
//        }
//
//        if (!wasPresent) {
//            // If the peer wasn't in our mesh, no need to send a PRUNE or set backoff
//            return;
//        }
//
//
//        // Send a PRUNE message so they know we've removed them
//        Message prune = createMessage(
//                -1,
//                Message.MSG_PRUNE,
//                this.nodeId,
//                peerID,
//                topicID,
//                null,
//                false,
//                -1,
//                -1,
//                now,
//                -1);
//
//        prune.setPrunePX(px);
//        publishMessage(prune, peerID, gossipSubId);
//
//        // Set backoff
//        PeerScoreInfo psi = peerScores.get(peerID);
//        if (psi != null) {
//            PeerScoreInfo.TopicScores ts =
//                    psi.topicScoresMap.computeIfAbsent(topicID, k -> new PeerScoreInfo.TopicScores());
//            ts.underDelivery += Math.max(0, ts.meshMsgExpected - ts.meshMsgDelivered);  // P3b
//            long backoff = 60000; // 1 minute
//            psi.pruneBackoffUntil = now + backoff;
//
//            if (isDEBUG) {
//                System.out.println("[DEBUG prunePeer] Setting pruneBackoffUntil="
//                        + psi.pruneBackoffUntil
//                        + " for peer " + peerID);
//            }
//        }
//    }

    public void prunePeer(BigInteger peerID, String topicID) {
        final long now = CommonState.getTime();

        Set<BigInteger> meshPeers = meshPeersByTopic.computeIfAbsent(topicID, k -> new HashSet<>());

        boolean wasPresent = meshPeers.remove(peerID);

        if (isDEBUG) {
            System.out.println("[DEBUG prunePeer] Node " + nodeId
                    + " PRUNE peer " + peerID
                    + " topic=" + topicID
                    + " t=" + now
                    + " wasPresent=" + wasPresent
                    + " meshSize(after)=" + meshPeers.size());
        }

        unmarkDirections(topicID, peerID); // clean inbound/outbound
        if (!wasPresent) {
            return;
        }


        Set<BigInteger> all = topicNodes.getOrDefault(topicID, Collections.emptySet());
        List<BigInteger> candidates = new ArrayList<>();
        for (BigInteger q : all) {
            if (q.equals(peerID)) continue;
            if (q.equals(nodeId)) continue;
            if (meshPeers.contains(q)) continue;

            PeerScoreInfo psiQ = peerScores.get(q);
            boolean inBackoff = false;
            if (psiQ != null) {

                PeerScoreInfo.TopicScores tsQ =
                        (psiQ.topicScoresMap != null) ? psiQ.topicScoresMap.get(topicID) : null;
                if (tsQ != null && tsQ.pruneBackoffUntil > now) {
                    inBackoff = true;
                } else if (psiQ.pruneBackoffUntil > now) {
                    inBackoff = true;
                }
            }
            if (inBackoff) continue;

            candidates.add(q);
        }

        if (candidates.isEmpty()) {
            for (BigInteger q : all) {
                if (!q.equals(peerID) && !q.equals(nodeId)) {
                    candidates.add(q);
                }
            }
        }

        for (BigInteger p : candidates) addPeerScoreIfAbsent(p);

        candidates.sort((a, b) -> {
            double sb = computeScore(peerScores.get(b));
            double sa = computeScore(peerScores.get(a));
            return Double.compare(sb, sa);
        });

        final int PX_LIMIT = D;
        List<BigInteger> px = candidates.subList(0, Math.min(PX_LIMIT, candidates.size()));

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
                -1
        );
        prune.setPrunePX(px);
        publishMessage(prune, peerID, gossipSubId);

        PeerScoreInfo psi = peerScores.get(peerID);
        if (psi != null) {
//            PeerScoreInfo.TopicScores ts =
//                    psi.topicScoresMap.computeIfAbsent(topicID, k -> new PeerScoreInfo.TopicScores());
            PeerScoreInfo.TopicScores ts = psi.getOrCreate(topicID);
            ts.underDelivery += Math.max(0, ts.meshMsgExpected - ts.meshMsgDelivered);

            final long backoff = 60_000L; // 1 phút
            if (ts != null) {
                ts.pruneBackoffUntil = now + backoff;
            } else {
                psi.pruneBackoffUntil = now + backoff;
            }

            if (isDEBUG) {
                long bo = (ts != null) ? ts.pruneBackoffUntil : psi.pruneBackoffUntil;
                System.out.println("[DEBUG prunePeer] Set PRUNE backoff until " + bo
                        + " for peer " + peerID + " on topic " + topicID);
            }
        }
    }

    private void maybeOpportunisticGraft(String topicId, int pid) {
        Set<BigInteger> mesh = meshPeersByTopic.getOrDefault(topicId, Collections.emptySet());
        if (mesh.isEmpty()) return;

        for (BigInteger p : mesh) addPeerScoreIfAbsent(p);
        final long now = CommonState.getTime();

        List<Double> scores = new ArrayList<>(mesh.size());
        for (BigInteger p : mesh) {
//            scores.add(computeScore(peerScores.getOrDefault(p, new PeerScoreInfo(p))));
            scores.add(computeScore(peerScores.get(p)));
        }
        Collections.sort(scores);
        double median = (scores.size() % 2 == 1)
                ? scores.get(scores.size() / 2)
                : 0.5 * (scores.get(scores.size() / 2 - 1) + scores.get(scores.size() / 2));

        if (median >= OP_GRAFT_MEDIAN_THRESHOLD) return;

        java.util.function.Predicate<BigInteger> isInBackoff = (peer) -> {
            PeerScoreInfo psi = peerScores.get(peer);
            if (psi == null) return false;
            PeerScoreInfo.TopicScores ts =
                    (psi.topicScoresMap != null) ? psi.topicScoresMap.get(topicId) : null;
            if (ts != null && ts.pruneBackoffUntil > now) return true;
            return psi.pruneBackoffUntil > now;
        };

        Set<BigInteger> outbound = outboundByTopic.computeIfAbsent(topicId, k -> new HashSet<>());
        Set<BigInteger> inbound  = inboundByTopic.computeIfAbsent(topicId,  k -> new HashSet<>());

        boolean allowExceed = false;
        try { allowExceed = ALLOW_EXCEED_D_HIGH_ON_DOUT; } catch (Throwable ignore) {}

        java.util.function.Supplier<BigInteger> worstInboundSupplier = () -> {
            BigInteger worstInbound = null;
            double worstScore = Double.POSITIVE_INFINITY;
            for (BigInteger q : mesh) {
                if (inbound.contains(q) && !outbound.contains(q)) {
//                    double s = computeScore(peerScores.getOrDefault(q, new PeerScoreInfo(q)));
                    double s = computeScore(peerScores.get(q));
                    if (s < worstScore) { worstScore = s; worstInbound = q; }
                }
            }
            return worstInbound;
        };

        int remaining = OP_GRAFT_K;

        List<BigInteger> inboundOnly = new ArrayList<>();
        for (BigInteger p : mesh) {
            if (!outbound.contains(p) && inbound.contains(p) && !isInBackoff.test(p)) {
                inboundOnly.add(p);
            }
        }
        for (BigInteger p : inboundOnly) addPeerScoreIfAbsent(p);
        inboundOnly.sort((a, b) -> {
            double sb = computeScore(peerScores.get(b));
            double sa = computeScore(peerScores.get(a));
            return Double.compare(sb, sa);
        });

        for (BigInteger p : inboundOnly) {
            if (remaining <= 0) break;

            addPeerScoreIfAbsent(p);
            Message graft = createMessage(/* id */ -1, Message.MSG_GRAFT, nodeId, p,
                    topicId, null, false, -1, -1, now, /*via*/ -7);
            publishMessage(graft, p, pid);
            markOutbound(topicId, p);
            remaining--;
        }
        if (remaining <= 0) return;

        Set<BigInteger> disallow = new HashSet<>(mesh);
        disallow.add(nodeId);
        List<BigInteger> pick = selectHighScorePeersForTopic(topicId, disallow, remaining * 3);

        List<BigInteger> outsideMesh = new ArrayList<>();
        List<BigInteger> inMeshButNotOutbound = new ArrayList<>();
        for (BigInteger p : pick) {
            if (p.equals(nodeId)) continue;
            if (outbound.contains(p)) continue;
            if (isInBackoff.test(p)) continue;

            if (!mesh.contains(p)) outsideMesh.add(p);
            else if (inbound.contains(p) && !outbound.contains(p)) inMeshButNotOutbound.add(p);
        }

        for (BigInteger p : outsideMesh) {
            if (remaining <= 0) break;

//            if (!allowExceed && mesh.size() >= D_HIGH) {
//                BigInteger worstInbound = worstInboundSupplier.get();
//                if (worstInbound != null) {
//                    prunePeer(worstInbound, topicId);
//                    mesh.remove(worstInbound);
//                    unmarkDirections(topicId, worstInbound);
//                } else {
//                    continue;
//                }
//            }
            if (!allowExceed && mesh.size() >= D_HIGH) {
                BigInteger victim = pickVictimAtHigh(mesh, inbound, outbound, topicId, now);
                if (victim != null) {
                    prunePeer(victim, topicId);
                    mesh.remove(victim);
                    unmarkDirections(topicId, victim);
                } else {
                    continue;
                }
            }


            mesh.add(p);
            addPeerScoreIfAbsent(p);
            Message graft = createMessage(/* id */ -1, Message.MSG_GRAFT, nodeId, p,
                    topicId, null, false, -1, -1, now, /*via*/ -7);
            publishMessage(graft, p, pid);
            markOutbound(topicId, p);
            remaining--;
        }
        if (remaining <= 0) return;

        for (BigInteger p : inMeshButNotOutbound) {
            if (remaining <= 0) break;

//            if (!allowExceed && mesh.size() >= D_HIGH) {
//                BigInteger worstInbound = worstInboundSupplier.get();
//                if (worstInbound != null && !worstInbound.equals(p)) {
//                    prunePeer(worstInbound, topicId);
//                    mesh.remove(worstInbound);
//                    unmarkDirections(topicId, worstInbound);
//                } else {
//                    continue;
//                }
//            }

            if (!allowExceed && mesh.size() >= D_HIGH) {
                BigInteger victim = pickVictimAtHigh(mesh, inbound, outbound, topicId, now);
                if (victim != null) {
                    prunePeer(victim, topicId);
                    mesh.remove(victim);
                    unmarkDirections(topicId, victim);
                } else {
                    continue;
                }
            }


            addPeerScoreIfAbsent(p);
            Message graft = createMessage(/* id */ -1, Message.MSG_GRAFT, nodeId, p,
                    topicId, null, false, -1, -1, now, /*via*/ -7);
            publishMessage(graft, p, pid);
            markOutbound(topicId, p);
            remaining--;
        }
    }



    private void removeExcessPeers(String topicID, int toPrune, int pid) {
        if (toPrune <= 0) return;

        Set<BigInteger> mesh = meshPeersByTopic.computeIfAbsent(topicID, k -> new HashSet<>());
        if (mesh.isEmpty()) return;

        Set<BigInteger> outbound = outboundByTopic.computeIfAbsent(topicID, k -> new HashSet<>());
        Set<BigInteger> inbound  = inboundByTopic.computeIfAbsent(topicID,  k -> new HashSet<>());

        for (BigInteger p : mesh) addPeerScoreIfAbsent(p);

        List<BigInteger> inboundOnly = new ArrayList<>();
        for (BigInteger p : mesh) {
            if (inbound.contains(p) && !outbound.contains(p)) {
                inboundOnly.add(p);
            }
        }
        inboundOnly.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));

        int pruned = 0;

        for (BigInteger victim : inboundOnly) {
            if (pruned >= toPrune) break;
            prunePeer(victim, topicID);  // đã xử lý PX + backoff + unmarkDirections bên trong
            pruned++;
        }
        if (pruned >= toPrune) return;


        List<BigInteger> outboundStillInMesh = new ArrayList<>();
        for (BigInteger p : mesh) {
            if (outbound.contains(p)) outboundStillInMesh.add(p);
        }
        outboundStillInMesh.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));

        int currentOutbound = outboundCount(topicID);
        for (BigInteger victim : outboundStillInMesh) {
            if (pruned >= toPrune) break;

            if (currentOutbound <= D_OUT) break;

            prunePeer(victim, topicID);
            pruned++;
            currentOutbound--;
        }


        if (pruned < toPrune) {
            List<BigInteger> rest = new ArrayList<>(meshPeersByTopic.getOrDefault(topicID, Collections.emptySet()));
            rest.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));

            for (BigInteger victim : rest) {
                if (pruned >= toPrune) break;

                boolean isOutbound = outbound.contains(victim);
                if (isOutbound && outboundCount(topicID) <= D_OUT) {
                    continue;
                }
                prunePeer(victim, topicID);
                pruned++;
            }
        }
    }


//    private void removeExcessPeers(String topicID, int toPrune) {
//        Set<BigInteger> mesh = meshPeersByTopic.get(topicID);
//        if (mesh == null || toPrune <= 0) return;
//
//        /* ensure scores exist, then sort by ascending score */
//        mesh.forEach(this::addPeerScoreIfAbsent);
//        List<BigInteger> sorted = new ArrayList<>(mesh);
//        sorted.sort(Comparator.comparingDouble(p -> computeScore(peerScores.get(p))));
//
//        /* PRUNE the ‘toPrune’ worst peers */
//        for (int i = 0; i < toPrune && i < sorted.size(); i++) {
//            prunePeer(sorted.get(i), topicID);
//        }
//    }

    private void addPeerScoreIfAbsent(BigInteger peerID) {
        peerScores.computeIfAbsent(peerID, id -> new PeerScoreInfo(peerID));
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

    public double getAverageBandwidthKbps() {   // name stays, semantics change to kbps
        if (totalTransmissionTime <= 0) {
            return 0.0;
        }
        return (totalDataTransmitted * 8.0) / (double) totalTransmissionTime;
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
//        for (Message c : custodyData2)
//            if (c.id == want.id)
//                return c;
        return null;
    }

    public void publishMessage(Message m, BigInteger destId, int myPid) {

        // Malicious
        // isMaliciousNode() && !m.src.equals(this.nodeId) // forwarding only ommisionb
        if (isOmissionNode() && !isFloodingNode()) {   // keep pure-omission only
            if (isDEBUG) System.out.println("[OMISSION] node " + nodeId
                    + " dropped fwd of msg " + m.id);
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

//        while (!messageTransmissionDelayQueue.isEmpty()
//                && CommonState.getTime() > messageTransmissionDelayQueue.get(0)) {

//        while (!messageTransmissionDelayQueue.isEmpty()
//                && CommonState.getTime() >= messageTransmissionDelayQueue.get(0)) {
//            messageQueue.remove(0);
//            messageTransmissionDelayQueue.remove(0);
//        }
//
//        long queuingDelay = messageQueue.isEmpty()
//                ? 0
//                : messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1)
//                - CommonState.getTime();

//        int messageSize = calculateMessageSize(m);
//        long transmissionDelay = (long) Math.ceil((double) messageSize / (double) (bandwidth / 1000));
//        long propagationDelay = latency;
//        long totalDelay = queuingDelay + transmissionDelay + propagationDelay;
//
//        totalDataTransmitted += messageSize;
//        totalTransmissionTime += totalDelay;
//
//        EDSimulator.add(totalDelay, m, dest, myPid);

        if (latency < 0) {
            if (isDEBUG) {
                System.out.println("[WARN] Negative latency " + latency +
                        " from " + this.nodeId + " to " + destId + ", clamping to 0");
            }
            latency = 0;
        }

        while (!messageTransmissionDelayQueue.isEmpty()
                && CommonState.getTime() >= messageTransmissionDelayQueue.get(0)) {
            messageQueue.remove(0);
            messageTransmissionDelayQueue.remove(0);
        }

        long queuingDelay = messageQueue.isEmpty()
                ? 0
                : messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1)
                - CommonState.getTime();


        int messageSize = calculateMessageSize(m);
        // Convert bandwidth (bits/s) -> bytes/ms
        double bytesPerMs = (bandwidth / 8.0) / 1000.0;
        long transmissionDelay = (long) Math.ceil(messageSize / bytesPerMs);

        long propagationDelay = latency;
        long totalDelay = queuingDelay + transmissionDelay + propagationDelay;

        if (totalDelay < 0) {
            if (isDEBUG) {
                System.out.println("[WARN] totalDelay < 0 (" + totalDelay +
                        ") for msg " + m.id + ", clamping to 0");
            }
            totalDelay = 0;
        }

        totalDataTransmitted += messageSize;
        // For average bandwidth, only count serialization time:
        totalTransmissionTime += transmissionDelay;

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


//    public void createWholeRowOrColumnSeed(Message m, int rowOrColNum) {
//        byte[][] body = m.isRow ? block.getRowData(rowOrColNum) : block.getColumnData(rowOrColNum);
//        Message newSeed = createMessage(
//                -1, 3, nodeId, nodeId, m.messageTopicID, body,
//                m.isRow, m.rowOrColumnNumber, -1, CommonState.getTime(), -1);
//        custodyData1.add(newSeed);
//        samplingStarter();
//    }



    public void createWholeRowOrColumnSeed(Message m, int rowOrColNum) {
        byte[][] body = m.isRow ? block.getRowData(rowOrColNum) : block.getColumnData(rowOrColNum);

        Message newSeed = createMessage(
                -1, 3, nodeId, nodeId, m.messageTopicID, body,
                m.isRow, m.rowOrColumnNumber, -1, CommonState.getTime(), -1);

        String key = (m.isRow ? "row" : "column") + rowOrColNum;

        if (key.equals(custody1)) {
            custodyData1.add(newSeed);
        }
//        if (key.equals(custody2)) {
//            custodyData2.add(newSeed);
//        }
        samplingStarter();
    }


        public void handleReceivedPart(Message m, int myPid) {
        String s = m.isRow ? "row" : "column";
        String key = s + m.rowOrColumnNumber;
//        int threshold = 1;
//        int threshold = (SHARD_AMOUNT + 1) / 2;

        if (custody1.equals(key)) {
            processCustody(m, custody1Parts, threshold);
        }
//        else if (custody2.equals(key)) {
//            processCustody(m, custody2Parts, threshold);
//        }
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
            noOfSeedPartsReceived++;

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

        }
//        else if (custody2.equals(key)) { // message belongs to custody‑2
//            if (custodyData2.contains(m)) {
//                if (isDEBUG) {
//                    System.out.printf("[DUP] node %s got duplicate shard id=%d at t=%d%n",
//                            nodeId, m.id, CommonState.getTime());
//                }
//            } else {
//                custodyData2.add(m);
//            }
//
//        }
        else { // not my custody – ignore
            return;
        }

        long now = CommonState.getTime();
        messageArrivalTimeFromBP.add(now);
        messageDelayTimeFromBP.add(now - m.timestamp);
        seedArrivalTimeStore.add(now);
    }

    public void samplingStarter() {
        if (samplingStarted || !blockIsReset)
            return;

        boolean shouldStart = false;

        if (distributionStrategy == 3) {
            // Start sampling if EITHER custodyData1 OR custodyData2 has 1 piece
            if (custodyData1.size() >= 1 ) {
                shouldStart = true;
            }
        } else if (distributionStrategy == 2) {
            if (custodyData1.size() >= 1 ) {
                shouldStart = true;
            }
        }

        if (shouldStart) {
            samplingStarted = true;
            blockIsReset = false;
            startSampling();
        }
    }


    public void handleIHave(Message m, int myPid) {
        HashSet<Long> seenFromThisPeer =
                seenIHaveByPeer.computeIfAbsent(m.src, k -> new HashSet<>());

        if (!seenFromThisPeer.add(m.id)) {
            duplicateIHaveMessage++;
            return;    // drop repeats from the same sender
        }

        if (messageCache.containsKey(m.id)) {
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

        if (lastAdvertisedTime.getOrDefault(m.id, 0L) + ADVERTISEMENT_TTL > now) {
            return;                           // recently advertised → done
        }
        lastAdvertisedTime.put(m.id, now);

//        Message advert = createMessage(
//                m.id, Message.MSG_IHAVE,
//                nodeId,               /* src = me (re-originated) */
//                null,                 /* dest = fill per-peer below */
//                m.messageTopicID,
//                /* body */ null,
//                m.isRow, m.rowOrColumnNumber, m.partNumber,
//                now, -6);
//
//        sendMessageToPeers(advert, myPid, m.messageTopicID,
//                /* avoid = */ nodeId,          // don’t echo back to self
//                m.src);                        // don’t send to original sender


        incrementDelivered(m.src, m.messageTopicID);
        // edger push

        if (m.body != null) {             // only cache once we have the full payload
            storeInEphemeralCache(m);
        }

        // Lazy gossip
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
        if (m.body != null) {
            storeInEphemeralCache(m);
        }
        samplingStarter();
    }

    private void handleTypeMessage(Message m, int myPid) {
        String key = (m.isRow ? "row" : "column") + m.rowOrColumnNumber;

        if (distributionStrategy == 3) {
            if (!custodyData1.contains(m) ) {
                if (custody1.equals(key) ) {
                    if (m.body == null) {
                        requestMissingData(m, myPid, custodyData1.isEmpty() ? custody1 : null);
                    } else {
                        handleReceivedRowOrCol(m, myPid);
                    }
                }
            }
        } else if (distributionStrategy == 2) {
            if (!custodyData1.contains(m)) {
                if (custody1.equals(key)) {
                    if (m.body == null) {
                        requestMissingPart(m, myPid, custody1Parts.size(), custody1);
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
    private void requestMissingPart(Message m, int myPid, int custody1Size, String custody1) {
//        int threshold = (SHARD_AMOUNT + 1) / 2;
//        int threshold = 1;
        String key = (m.isRow ? "row" : "column") + m.rowOrColumnNumber;

        if (custody1.equals(key) && custody1Size < threshold) {
            sendIWantMessage(m, myPid);
        }
//        else if (custody2.equals(key) && custody2Size < threshold) {
//            sendIWantMessage(m, myPid);
//        }
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
//                                duplicateData++;
            } else {
                markInvalidMessage(m.src, m.messageTopicID);
                if (isDEBUG) {
                    System.out.printf("[MISMATCH] proposer saw conflicting body for msg=%d at t=%d%n",
                            m.id, CommonState.getTime());
                }
            }
            return;
        }

//        if (!seenShards.add(m.id)) {
//            duplicateShards++;
//            return;
//        }

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

    // Edger push
//    private static final boolean IHAVE_FIRST = false;
//    private void sendDataToMesh(Message m, int myPid, BigInteger avoidPeer) {
//
//        /* -----------   (a) remember the full body locally   ----------- */
//        storeInEphemeralCache(m);
//
//        /* -----------   (b) choose what to send to mesh peers   ----------- */
//        if (IHAVE_FIRST) {
//            if (m.body != null ) {
//                storeInEphemeralCache(m);   // cache DATA by id
//            }
//            /* send only metadata now – peers will IWANT it if needed */
//            Message advert = createMessage(
//                    m.id, Message.MSG_IHAVE,
//                    nodeId, null,
//                    m.messageTopicID,
//                    null,                      // no payload
//                    m.isRow, m.rowOrColumnNumber, m.partNumber,
//                    CommonState.getTime(), -6);
//
//            sendMessageToPeers(advert, myPid, m.messageTopicID,
//                    (avoidPeer == null ? nodeId : avoidPeer),
//                    nodeId);
//        } else {
//            if (m.body != null ) {
//                storeInEphemeralCache(m);   // cache DATA by id
//            }
//            /* legacy eager full-DATA path (kept for toggling) */
//            Message full = createMessage(
//                    m.id, Message.MSG_DATA,
//                    nodeId, m.dest, m.messageTopicID,
//                    m.body,
//                    m.isRow, m.rowOrColumnNumber, m.partNumber,
//                    CommonState.getTime(), -6);
//
//            sendMessageToPeers(full, myPid, m.messageTopicID,
//                    (avoidPeer == null ? nodeId : avoidPeer),
//                    nodeId);
//        }
//    }


    private void sendDataToMesh(Message m, int myPid, BigInteger avoidPeer) {

        /* 1. full body to every mesh peer (MSG_DATA) */

        Message full = createMessage(
                m.id, Message.MSG_DATA,
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

        /* 1 ──────────────────────────────────────────────────────
         *    Fast duplicate filter (same uid, second arrival)
         */
        if (!seenShards.add(m.id)) {       // already saw this uid
            duplicateShards++;             // metric
            return;                        // drop body
        }
        uniqueShards++;

        /* 2 ──────────────────────────────────────────────────────
         *    Cache lookup: did we store a full body/data before?
         */
        Message cached = messageCache.get(m.id);
        if (cached != null && cached.body != null) {
            if (!samePayload(cached.body, m.body)) {
                // same ID but different bytes  → mark invalid
                if (isDEBUG) {
                    System.out.printf(
                            "[MISMATCH] node %s got conflicting body id=%d t=%d%n",
                            nodeId, m.id, CommonState.getTime());
                }
                /* markInvalidMessage(m.src, m.messageTopicID); */
            }
            return;                       // we already processed the good copy
        }

        /* 3 ──────────────────────────────────────────────────────
         *    First *full* copy we’ve seen  → store & eager-push
         */
        messageCache.put(m.id, m);        // cache full body

        sendDataToMesh(m, myPid, m.src);  // eager push to D peers (skip sender)

        /* 4 ──────────────────────────────────────────────────────
         *    Apply shard data according to distribution strategy
         */
        if (distributionStrategy == 3) {
            handleReceivedRowOrCol(m, myPid);
        } else if (distributionStrategy == 2) {
            handleReceivedPart(m, myPid);
        }

        /* 5 ──────────────────────────────────────────────────────
         *    Optional: kick off DAS sampling if conditions met
         */
        samplingStarter();
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

//        PeerScoreInfo.TopicScores ts = psi.topicScoresMap.computeIfAbsent(topic, k -> new PeerScoreInfo.TopicScores());
        PeerScoreInfo.TopicScores ts = psi.getOrCreate(topic);


        ts.firstMessageDeliveries += 1;
        ts.meshMsgDelivered       += 1;
    }

    public void markInvalidMessage(BigInteger srcPeer, String topic) {
        PeerScoreInfo psi = peerScores.get(srcPeer);
        if (psi == null)
            return;

//        PeerScoreInfo.TopicScores ts = psi.topicScoresMap.computeIfAbsent(topic, k -> new PeerScoreInfo.TopicScores());
        PeerScoreInfo.TopicScores ts = psi.getOrCreate(topic);

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
            psi = new PeerScoreInfo(srcPeer);
            peerScores.put(srcPeer, psi);
        }
//        PeerScoreInfo.TopicScores ts = psi.topicScoresMap.computeIfAbsent(topic, k -> new PeerScoreInfo.TopicScores());
        PeerScoreInfo.TopicScores ts = psi.getOrCreate(topic);
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

        int effectiveParallelAsk = getAdaptiveParallelism();
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
            if (sentNow == effectiveParallelAsk)
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



//    public void handleSampleRequest(Message m, int myPid) {
//
//        boolean requestedRow = m.isRow;
//        int rowOrColNumb = m.rowOrColumnNumber;
//        int idx = Integer.parseInt((String) m.body);
//
//        BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode
//                .getProtocol(gossipSubId))).nodeId;
//
//        if (!custody1.isEmpty()) {
//            for (Message custodyMessage : custodyData1) {
//                if (m.isRow == custodyMessage.isRow && m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber
//                        && m.partNumber == custodyMessage.partNumber) {
//                    byte[][] data = (byte[][]) custodyData1.get(0).body;
//                    byte sampleDataResp = data[0][idx];
//                    Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, this.nodeId, m.src,
//                            m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber,
//                            m.partNumber, m.timestamp, m.typeID);
//                    this.publishMessage(sampleResponse, m.src, gossipSubId);
//                    return;
//                }
//            }
//        }
//        if (!custody2.isEmpty()) {
//            for (Message custodyMessage : custodyData2) {
//                if (m.isRow == custodyMessage.isRow && m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber
//                        && m.partNumber == custodyMessage.partNumber) {
//                    byte[][] data = (byte[][]) custodyData2.get(0).body;
//                    byte sampleDataResp = data[0][idx];
//                    Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, this.nodeId, m.src,
//                            m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber,
//                            m.partNumber, m.timestamp, m.typeID);
//                    this.publishMessage(sampleResponse, m.src, gossipSubId);
//                    return;
//                }
//            }
//        }
//    }

    public void handleSampleRequest(Message m, int myPid) {

        boolean requestedRow = m.isRow;
        int rowOrColNumb = m.rowOrColumnNumber;
        int idx = Integer.parseInt((String) m.body);

        BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId))).nodeId;

        if (!custody1.isEmpty()) {
            for (Message custodyMessage : custodyData1) {
                boolean sameRowCol = (m.isRow == custodyMessage.isRow) &&
                        (m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber);

                boolean partMatchesOrIsSeed = (custodyMessage.partNumber == -1) ||
                        (m.partNumber == custodyMessage.partNumber);

                if (sameRowCol && partMatchesOrIsSeed) {
                    byte[][] data = (byte[][]) custodyMessage.body;
                    byte sampleDataResp = data[0][idx];
                    Message sampleResponse = createMessage(
                            m.id, Message.MSG_SAMPLE_DATA_RESPONSE, this.nodeId, m.src,
                            m.messageTopicID, Integer.toString(sampleDataResp),
                            m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.typeID);
                    this.publishMessage(sampleResponse, m.src, gossipSubId);
                    return;
                }
            }

        }
//        if (!custody2.isEmpty()) {
//            for (Message custodyMessage : custodyData2) {
//                boolean sameRowCol = (m.isRow == custodyMessage.isRow) &&
//                        (m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber);
//
//                boolean partMatchesOrIsSeed = (custodyMessage.partNumber == -1) ||
//                        (m.partNumber == custodyMessage.partNumber);
//
//                if (sameRowCol && partMatchesOrIsSeed) {
//                    byte[][] data = (byte[][]) custodyMessage.body;
//                    byte sampleDataResp = data[0][idx];
//                    Message sampleResponse = createMessage(
//                            m.id, Message.MSG_SAMPLE_DATA_RESPONSE, this.nodeId, m.src,
//                            m.messageTopicID, Integer.toString(sampleDataResp),
//                            m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.typeID);
//                    this.publishMessage(sampleResponse, m.src, gossipSubId);
//                    return;
//                }
//            }
//        }
    }

    public void handleSampleResponse(Message m, int myPid) {
        sampleArrivalTime.add(CommonState.getTime());
        sampleDelayTime.add(CommonState.getTime() - m.timestamp);
        samplingDelayTimeStore.add(CommonState.getTime());
        noOfSamplesReceived++;
    }


    private int getAdaptiveParallelism() {
        if (!ADAPTIVE_SAMPLING) {
            return PARALLEL_ASK;
        }

        // Increase parallelism based on failures
        int effective = PARALLEL_ASK + sampleRequestUnsuccessful;

        // Cap at the limit
        return Math.min(effective, PARALLEL_ASK_HIGH);
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
            noOfSampleRequestsSent++;
            publishMessage(msgToResend, msgToResend.dest, myPid);

            peersim.GossipSub.Timeout newTimeout = new peersim.GossipSub.Timeout(1, destId, msgToResend.id);

            Node src = CustomDistribution.networkNodes.get(this.nodeId);
            Node dest = CustomDistribution.networkNodes.get(destId);
            long latency = transport.getLatency(src, dest);
            EDSimulator.add(4 * latency, newTimeout, src, gossipSubId);
            // EDSimulator.add(SAMPLE_REQ_TIMEOUT, newTimeout, src, gossipSubId);

        }
    }


//    private void handleTimeOut(peersim.GossipSub.Timeout timeoutEvent, int myPid) {
//        if (pendingIWant.remove(timeoutEvent.msgID)) {
//            noOfSampleRequestsSent--;
//        }
//
//        if (sentMsg.containsKey(timeoutEvent.msgID)) {
//
//            this.noOfSampleRequestsSent++;
//            sampleRequestUnsuccessful++;
//
//            Message sampleMsgSent = sentMsg.get(timeoutEvent.msgID);
//            sentMsg.remove(timeoutEvent.msgID);
//
//            BigInteger destId;
//            // Random random = new Random();
//            Random random = CommonState.r;
//            if (sampleMsgSent.isRow) {
//                ArrayList<BigInteger> rowHolders = rowCustodyNodes.get(sampleMsgSent.rowOrColumnNumber);
//                int sampleHolderNodeIdx = random.nextInt(rowHolders.size());
//                destId = rowHolders.get(sampleHolderNodeIdx);
//            } else {
//                ArrayList<BigInteger> colHolders = columnCustodyNodes.get(sampleMsgSent.rowOrColumnNumber);
//                int sampleHolderNodeIdx = random.nextInt(colHolders.size());
//                destId = colHolders.get(sampleHolderNodeIdx);
//            }
//
//            Message msgToResend = this.createMessage(
//                    -1,
//                    sampleMsgSent.type,
//                    this.nodeId,
//                    destId,
//                    sampleMsgSent.messageTopicID,
//                    sampleMsgSent.body,
//                    sampleMsgSent.isRow,
//                    sampleMsgSent.rowOrColumnNumber,
//                    sampleMsgSent.partNumber,
//                    0,
//                    sampleMsgSent.typeID);
//
//            sentMsg.put(msgToResend.id, msgToResend);
//            noOfSampleRequestsSent++;
//            publishMessage(msgToResend, msgToResend.dest, myPid);
//
//            peersim.GossipSub.Timeout newTimeout = new peersim.GossipSub.Timeout(1, destId, msgToResend.id);
//
//            Node src = CustomDistribution.networkNodes.get(this.nodeId);
//            Node dest = CustomDistribution.networkNodes.get(destId);
//            long latency = transport.getLatency(src, dest);
//            EDSimulator.add(4 * latency, newTimeout, src, gossipSubId);
//            // EDSimulator.add(SAMPLE_REQ_TIMEOUT, newTimeout, src, gossipSubId);
//
//        }
//    }

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
                m = (Message) event;

                if (this.isBlockProposerNode()) {
                    // --- BLOCK PROPOSER ---
                    System.out.println("I am the block producer");
                    System.out.println("Message sent from trafficGenerator at: " + m.timestamp);
                    System.out.println("Block producer started at: " + CommonState.getTime());

                    // 1. Generate a NEW block
                    block = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS"));

                    // *** ADD THIS BLOCK TO BROADCAST A RESET ***
                    System.out.println("[PROPOSER] Broadcasting RESET to network.");
                    Message resetMsg = createMessage(-1, Message.MSG_EMPTY,
                            this.nodeId, null, // dest is null for broadcast
                            null, null, false, -1, -1, CommonState.getTime(), -1);

                    // Send to ALL nodes (except self)
                    for (BigInteger peerId : CustomDistribution.networkNodes.keySet()) {
                        if (peerId.equals(this.nodeId)) continue;

                        // Create a copy for each peer
                        Message resetCopy = (Message) resetMsg.copy();
                        resetCopy.dest = peerId;

                        // Use publishMessage to send it
                        publishMessage(resetCopy, peerId, myPid);
                    }



                    if (distributionStrategy == 3) {
                        // ...
                    } else if (distributionStrategy == 2) {
                        seenShards.clear();
                        duplicateShards = uniqueShards = 0;
                        duplicateIHaveMessage = 0;

                        shardingBasedDistribution(CommonState.getTime());
                    }

                } else {
                    // This block is likely never hit, which is why we need the broadcast
                    resetNodeStateForNewBlock();
                }
                break;

//            case Message.MSG_BLOCK_PROPOSER:
//                System.out.println("I am the block producer");
//                m = (Message) event;
//                System.out.println("Message sent from trafficGenerator at: " + m.timestamp);
//                System.out.println("Block producer started at: " + CommonState.getTime());
//                block = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS"));
//                if (distributionStrategy == 3) {
////                    nCopiesDistributionStrategy();
//                } else if (distributionStrategy == 2) {
//                    seenShards.clear();
//                    duplicateShards = uniqueShards = 0;
//                    duplicateIHaveMessage = 0;
//                    shardingBasedDistribution();
//                }
//                EDSimulator.add(HEARTBEAT_PERIOD,
//                        new SimpleEvent(Message.MSG_HEARTBEAT), myNode, myPid);
//                break;

            case Message.MSG_SAMPLE_DATA_REQUEST:
                m = (Message) event;
                handleSampleRequest(m, myPid);
                break;

            case Message.MSG_SAMPLE_DATA_RESPONSE:
                m = (Message) event;
                if (sentMsg.containsKey(m.id)) {
                    sentMsg.remove(m.id);
                    pendingIWant.remove(m.id);
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

//                if (isFloodingNode()) {
//                    int rate = Configuration.getInt("FLOOD_RATE", 100);
//                    if (subscribedTopics.isEmpty()) {
//                        System.out.printf("[FLOOD-EMPTY] node=%s has no topics; skipping%n", nodeId);
//                    }
//                    for (int i = 0; i < rate; i++) {
//                        for (Topic t : subscribedTopics) {
//                            Message flood = buildFloodMsg(t.topicID);
//                            sendDataToMesh(flood, myPid, nodeId); // avoid echoing to self
//                            System.out.printf("[FLOOD-ATK] node=%s topic=%s t=%d%n",
//                                    nodeId, t.topicID, CommonState.getTime()); // newline flushes
//                        }
//                    }
//                }
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
                List<BigInteger> px = pruneMsg.getPrunePX();
                handlePrune(pruneMsg, myPid);
                break;

            case Message.MSG_EMPTY:
                if (!this.isBlockProposerNode()) {
                    resetNodeStateForNewBlock();
                }
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

//    private void nCopiesDistributionStrategy() {
//        // Load all the relevant configuration values
//        int numberOfCopiesToSend = Configuration.getInt("NUMBER_COPIES_DISTRIBUTED");
//        int numberOfRowsAndColsInTopic = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC");
//
//        int rowIndex = 0; // Tracks which row are distributing
//        int columnIndex = 0; // Tracks which column are distributing
//
//        // Retrieve the block to distribute
//        Block b = block;
//
//        // Just an example usage to log the block proposer’s ID
//        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
//                .getProtocol(gossipSubId));
//        System.out.println("Block proposer id is ------: " + iGossipBlockProposer.getNodeId());
//
//        boolean isRowTopic = true;
//        boolean isColTopic = false;
//
//        // Iterate over each topic in the distribution
//        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
//            // if (isDEBUG){
//            System.out.println();
//            System.out.println();
//            System.out.println("****");
//            System.out.println(topicEntry.getKey());
//
//            // }
//
//            Topic currentTopic = topicEntry.getValue();
//
//            int distributionCount = 0; // 'cnt' in original code
//            int nodeCounter = 0;
//            int copiesSent = 0;
//
//            Message baseMessage = null;
//
//            // Iterate over all nodes that are subscribed to this topic
//            for (Node node : currentTopic.topicMembers) {
//                GossipSubProtocol gossipProtocol = (GossipSubProtocol) node.getProtocol(gossipSubId);
//                // If the topic size is more than 1, decide if are distributing rows or
//                // columns
//                if (numberOfRowsAndColsInTopic != 1) {
//                    if (distributionCount < (numberOfRowsAndColsInTopic / 2)) {
//                        isRowTopic = true;
//                    } else {
//                        isRowTopic = false;
//                    }
//                }
//
//                // If distributed enough rows/columns for this topic, break
//                if (distributionCount >= numberOfRowsAndColsInTopic) {
//                    break;
//                }
//
//                // If already sent the desired number of copies
//                // AND the nodeCounter is not at the boundary for a new row/column holder
//                if (copiesSent >= numberOfCopiesToSend
//                        && (nodeCounter % SHARD_AMOUNT != 0)) {
//                    nodeCounter++;
//                    // Once hit the boundary again, reset things
//                    if (nodeCounter % SHARD_AMOUNT == 0) {
//                        copiesSent = 0;
//                        if (isRowTopic) {
//                            rowIndex++;
//                        } else {
//                            columnIndex++;
//                        }
//                        distributionCount++;
//                    }
//                    continue;
//                }
//
//                // Check row/column limits
//                if (isRowTopic && rowIndex == MAX_DIMENSION_SIZE) {
//                    // If reached the limit, move on
//                    continue;
//                }
//                if (!isRowTopic && columnIndex == MAX_DIMENSION_SIZE) {
//                    // If reached the limit, end the distribution entirely
//                    return;
//                }
//
//                // Determine which index are distributing (row or column)
//                int currentIndex = isRowTopic ? rowIndex : columnIndex;
//                BigInteger destinationId = gossipProtocol.getNodeId();
//
//                // Create/send the message
//                Message newMsg = createRowOrColumnMessageForDistribution(
//                        copiesSent,
//                        baseMessage,
//                        b,
//                        currentIndex,
//                        currentTopic.topicID,
//                        destinationId,
//                        isRowTopic);
//
//                // If this was the first time sent a copy, record it as our base
//                if (copiesSent == 0) {
//                    baseMessage = newMsg;
//                }
//
//                nodeCounter++;
//                copiesSent++;
//
//                // If reached the boundary for holders, reset counters
//                if (nodeCounter % SHARD_AMOUNT == 0) {
//                    copiesSent = 0;
//                    if (isRowTopic) {
//                        rowIndex++;
//                    } else {
//                        columnIndex++;
//                    }
//                    distributionCount++;
//                }
//
//                // Once again, if hit our max column limit, return
//                if (!isRowTopic && columnIndex == MAX_DIMENSION_SIZE) {
//                    return;
//                }
//            }
//
//            // Flip row/column topic for the next set
//            isRowTopic = !isRowTopic;
//            isColTopic = !isColTopic;
//        }
//
//        // Logging final counters and stats
//        System.out.println(rowIndex);
//        System.out.println(columnIndex);
//        System.out.println("*********Block proposer has sent the messages ********");
//        System.out.println("Data sent size " + totalDataTransmitted);
//        System.out.println("Data transmission time " + totalTransmissionTime);
//    }

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

    private static int shardUID(boolean isRow, int index, int part, long blockId) {
        // Use 17 bits from the blockId (time) to ensure uniqueness
        int timeHash = (int) (blockId & 0x1FFFF);

        return (isRow ? 1 << 31 : 0)           // MSB = axis
                | ((index & 0x1FF) << 22)         // 9 bits row/col#
                | ((part  & 0x1F) << 17)          // 5 bits part#
                | timeHash;                       // 17 bits for uniqueness
    }

//    private void shardingBasedDistribution(long blockId) {
//
//        final int divisions = SHARD_AMOUNT;          // A
//        final int kCopies   = Math.max(1, SHARD_COPIES); // Note: kCopies is unused in this model
//        final int rcCfg     = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 2);
//
//        boolean nextTopicIsRow = true;
//
//        int globalRow = 0, globalCol = 0;
//        Block blk = block;
//
//        // Iterate over topics as defined in your 'peerDAS' setup
//        for (Topic topic : CustomDistribution.topics.values()) {
//
//            int rowsPerTopic, colsPerTopic;
//            if (rcCfg == 1) {                       // row ⊕ col mode
//                rowsPerTopic = nextTopicIsRow ? 1 : 0;
//                colsPerTopic = nextTopicIsRow ? 0 : 1;
//                nextTopicIsRow = !nextTopicIsRow;
//            } else {                               // symmetric 1+1, 2+2… modes
//                rowsPerTopic = rcCfg / 2;
//                colsPerTopic = rowsPerTopic;
//            }
//
//            System.out.printf(
//                    "[SHARD-DIST] divisions=%d  k=%d  rows/cols per %s = %d/%d%n",
//                    divisions, kCopies, topic.topicID, rowsPerTopic, colsPerTopic);
//
//            int rowsSent = 0, colsSent = 0;
//            boolean sendRowNext = true;
//
//            // Loop while this topic still has rows or columns to distribute
//            while (rowsSent < rowsPerTopic || colsSent < colsPerTopic) {
//
//                boolean sendingRow = sendRowNext
//                        ? rowsSent < rowsPerTopic
//                        : colsSent >= colsPerTopic;
//
//                int shardIndex = sendingRow ? globalRow : globalCol;
//                int dimLen = sendingRow
//                        ? Configuration.getInt("NUMBER_OF_ROWS")
//                        : Configuration.getInt("NUMBER_OF_COLUMNS");
//
//                if (shardIndex >= dimLen) break;           // matrix exhausted
//
//                int partSize = dimLen / divisions;
//
//                // ------ [THIS IS THE CRITICAL FIX] ------
//
//                // 1. Get the pre-assigned list of custodians for this specific shard index.
//                List<BigInteger> custodianIds = sendingRow
//                        ? CustomDistribution.rowCustodyNodes.get(shardIndex)
//                        : CustomDistribution.columnCustodyNodes.get(shardIndex);
//
//                // 2. Check if custody was assigned correctly.
//                if (custodianIds == null || custodianIds.isEmpty()) {
//                    System.out.printf(
//                            "[SHARD-DIST ERROR] No custodians found for %s %d. Skipping.%n",
//                            sendingRow ? "row" : "col", shardIndex);
//
//                    // Increment counters to avoid infinite loop
//                    if (sendingRow) { globalRow++; rowsSent++; } else { globalCol++; colsSent++; }
//                    if (rowsPerTopic == colsPerTopic) sendRowNext = !sendRowNext;
//                    continue; // Skip to the next shard
//                }
//
//                // 'divisions' (SHARD_AMOUNT) should match the number of custodians assigned.
//                if (custodianIds.size() < divisions) {
//                    System.out.printf(
//                            "[SHARD-DIST WARNING] Custodian count mismatch for %s %d. Expected %d, found %d.%n",
//                            sendingRow ? "row" : "col", shardIndex, divisions, custodianIds.size());
//                }
//
//                // 3. Loop 'part' from 0 up to the number of custodians/divisions
//                int numPartsToSend = Math.min(divisions, custodianIds.size());
//
//                for (int part = 0; part < numPartsToSend; part++) {
//
//                    // 4. Get the specific custodian for this part from the map
//                    BigInteger custodianId = custodianIds.get(part);
//                    Node peer = CustomDistribution.networkNodes.get(custodianId);
//
//                    if (peer == null) {
//                        System.out.printf("[SHARD-DIST ERROR] Node not found for ID %s%n", custodianId);
//                        continue;
//                    }
//
//                    GossipSubProtocol dst =
//                            (GossipSubProtocol) peer.getProtocol(gossipSubId);
//
//                    // 5. Get the data slice for this part
//                    int start = part * partSize;
//                    int end   = Math.min(dimLen, start + partSize);
//
//                    byte[][] slice = sendingRow
//                            ? Arrays.copyOfRange(blk.getRowData(shardIndex),    start, end)
//                            : Arrays.copyOfRange(blk.getColumnData(shardIndex), start, end);
//
//                    // 6. Use the (now correct) unique ID function
//                    int uid = shardUID(sendingRow, shardIndex, part, blockId);
//
//                    // 7. Create the message
//                    Message msg = createMessage(
//                            uid, Message.MSG_DATA,
//                            this.nodeId, dst.nodeId,
//                            topic.topicID,
//                            slice,
//                            sendingRow,
//                            shardIndex,
//                            part,
//                            0,
//                            -1);
//
//                    // 8. Publish to the correct destination 'dst.nodeId'
//                    publishMessage(msg, dst.nodeId, gossipSubId);
//
//                    System.out.printf(
//                            "[SEED] %-3s %4d part=%02d → %s uid=%08X%n",
//                            sendingRow ? "row" : "col",
//                            shardIndex, part, dst.nodeId, uid);
//                }
//                // ------ [END OF FIX] ------
//
//                if (sendingRow) { globalRow++; rowsSent++; } else { globalCol++; colsSent++; }
//
//                if (rowsPerTopic == colsPerTopic) sendRowNext = !sendRowNext;
//            }
//        }
//        /* ── final sanity log ────────────────────────────────────────────── */
//        System.out.println("BLOCK ID = " + blockId);
//        System.out.printf("Finished: rows=%d  cols=%d%n", globalRow - 1, globalCol - 1);
//        System.out.println("*********Block proposer has sent the messages ********");
//        System.out.printf("Data sent size       : %d%n", totalDataTransmitted);
//        System.out.printf("Data transmission time: %d ms%n", totalTransmissionTime);
//        System.out.println("Malicious Rate: " + Configuration.getDouble("MALICIOUS_RATE"));
//        System.out.println("Seed Number: " + Configuration.getInt("random.seed"));
//        System.out.println("ROW/COL Holder: " + SHARD_AMOUNT);
//    }

    private void shardingBasedDistribution(long blockId) {

        final int divisions = SHARD_AMOUNT;          // A
        final int kCopies   = Math.max(1, SHARD_COPIES);
        final int rcCfg     = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 2);

        boolean nextTopicIsRow = true;

        int globalRow = 0, globalCol = 0;
        Block blk = block;

        for (Topic topic : CustomDistribution.topics.values()) {

            int rowsPerTopic, colsPerTopic;
            if (rcCfg == 1) {                       // row ⊕ col mode
                rowsPerTopic = nextTopicIsRow ? 1 : 0;
                colsPerTopic = nextTopicIsRow ? 0 : 1;
                nextTopicIsRow = !nextTopicIsRow;
            } else {                               // symmetric 1+1, 2+2… modes
                rowsPerTopic = rcCfg / 2;
                colsPerTopic = rowsPerTopic;
            }

            List<Node> members = topic.topicMembers.isEmpty()
                    ? List.of(CustomDistribution.blockProposerNode)
                    : topic.topicMembers;

            System.out.printf(
                    "[SHARD-DIST] divisions=%d  k=%d  rows/cols per %s = %d/%d%n",
                    divisions, kCopies, topic.topicID, rowsPerTopic, colsPerTopic);

            /* B: make sure we have enough unique recipients */
            if (members.size() < divisions * kCopies) {
                System.out.printf("[SHARD-DIST] Topic %s needs %d recipients but has only %d — "
                                + "will reuse nodes in round-robin%n",
                        topic.topicID, divisions * kCopies, members.size());
            }

            int rowsSent = 0, colsSent = 0;
            boolean sendRowNext = true;

            while (rowsSent < rowsPerTopic || colsSent < colsPerTopic) {

                boolean sendingRow = sendRowNext
                        ? rowsSent < rowsPerTopic
                        : colsSent >= colsPerTopic;

                int shardIndex = sendingRow ? globalRow : globalCol;
                int dimLen = sendingRow
                        ? Configuration.getInt("NUMBER_OF_ROWS")
                        : Configuration.getInt("NUMBER_OF_COLUMNS");

                if (shardIndex >= dimLen) break;           // matrix exhausted

                int partSize = dimLen / divisions;

                List<Node> shuffled = new ArrayList<>(members);
                Collections.shuffle(shuffled, CommonState.r);

                for (int part = 0; part < divisions; part++) {
                    for (int copy = 0; copy < kCopies; copy++) {

//                        Node peer = shuffled.get(part * kCopies + copy);  // no wrap-around
                        Node peer = shuffled.get((part * kCopies + copy) % shuffled.size());

                        GossipSubProtocol dst =
                                (GossipSubProtocol) peer.getProtocol(gossipSubId);

                        int start = part * partSize;
                        int end   = Math.min(dimLen, start + partSize);   // C

                        byte[][] slice = sendingRow
                                ? Arrays.copyOfRange(blk.getRowData(shardIndex),    start, end)
                                : Arrays.copyOfRange(blk.getColumnData(shardIndex), start, end);

                        int uid = shardUID(sendingRow, shardIndex, part, blockId);

//                        int uid = shardUID(sendingRow, shardIndex, part); // D

//                        Message msg = createMessage(
//                                uid, Message.MSG_DATA,
//                                this.nodeId, dst.nodeId,
//                                topic.topicID,
//                                slice,
//                                sendingRow,
//                                shardIndex,
//                                part,
//                                copy,
//                                -1);

//                        publishMessage(msg, dst.nodeId, gossipSubId);

                        Message msg = createMessage(
                                uid, Message.MSG_DATA,
                                this.nodeId, dst.nodeId,
                                topic.topicID,
                                slice,
                                sendingRow,
                                shardIndex,
                                part,
                                0,
                                -1);

//                        for (BigInteger peerId : recipientSet(topic)) {
//                            if (peerId.equals(this.nodeId)) {
//                                continue;     // skip self
//                            }
//                            publishMessage(msg, peerId, gossipSubId);
//                        }
//                        publishMessage(msg, this.nodeId, gossipSubId);
                        publishMessage(msg, dst.nodeId, gossipSubId);


                        rememberCustodian(sendingRow, shardIndex, dst.nodeId);

                        System.out.printf(
                                "[SEED] %-3s %4d part=%02d copy=%d/%d → %s uid=%08X%n",
                                sendingRow ? "row" : "col",
                                shardIndex, part, copy + 1, kCopies, dst.nodeId, uid);
                    }
                }

                if (sendingRow) { globalRow++; rowsSent++; } else { globalCol++; colsSent++; }

                if (rowsPerTopic == colsPerTopic) sendRowNext = !sendRowNext;
            }
        }
        /* ── final sanity log ────────────────────────────────────────────── */
        System.out.println("BLOCK ID = " + blockId);
        System.out.printf("Finished: rows=%d  cols=%d%n", globalRow - 1, globalCol - 1);
        System.out.println("*********Block proposer has sent the messages ********");
        System.out.printf("Data sent size       : %d%n", totalDataTransmitted);
        System.out.printf("Data transmission time: %d ms%n", totalTransmissionTime);
        System.out.println("Malicious Rate: " + Configuration.getDouble("MALICIOUS_RATE"));
        System.out.println("Seed Number: " + Configuration.getInt("random.seed"));
        System.out.println("ROW/COL Holder: " + SHARD_AMOUNT);
    }

//    public static Block block = new Block(
//            Configuration.getInt("NUMBER_OF_ROWS"),
//            Configuration.getInt("NUMBER_OF_COLUMNS"));

    public void resetNodeStateForNewBlock() {
        // 1. Reset Statistical Lists
        messageArrivalTimeFromBP.clear();       // seedArrivalTimes
        messageDelayTimeFromBP.clear();         // seedMessageDelayTimes
        sampleArrivalTime.clear();              // sampleArrivalTimes
        sampleDelayTime.clear();                // sampleMessageDelayTimes
        seedPartArrivalTimeFromPeer.clear();    // seedPartArrivalTimes
        seedPartDelayTimeFromPeer.clear();      // seedPartDelayTimes

        // 2. Reset Statistical Counters
        noOfSampleRequestsSent = 0;
        noOfSamplesReceived = 0;
        sampleRequestUnsuccessful = 0;
        noOfSeedPartsReceived = 0;
        duplicateIHaveMessage = 0;
        duplicateShards = 0;
        uniqueShards = 0;

        // 3. Reset IncrementalStats objects
        seedArrivalTimeStore.reset();
        samplingDelayTimeStore.reset();
        seedPartArrivalTimeStore.reset();

        // 4. Reset Bandwidth Counters
        totalDataTransmitted = 0;
        totalTransmissionTime = 0;

        // 5. *** CRITICAL: Reset Functional State ***
        // This is what actually allows a new block to be processed.
        seenShards.clear();
        seenIHaveByPeer.clear();
        messageCache.clear();
        IWANTmessageCache.clear();
        ephemeralCache.clear();
        pendingIWant.clear();

        // Clear old custody data
        custodyData1.clear();
//        custodyData2.clear();
        custody1Parts.clear();
//        custody2Parts.clear();

        // Re-enable sampling for the new block
        samplingStarted = false;
        blockIsReset = true;
    }

    // ---- Safe stubs (keep or replace with your real impls) ----
    private void advertisePendingIHave(int pid) { /* no-op for now */ }
    private void decayPeerTopicCountersIfAny() { /* no-op for now */ }
    private void decayScoresIfAny() { /* no-op for now */ }



    private Set<BigInteger> recipientSet(Topic topic) {

        // 1 – mesh if available
        Set<BigInteger> mesh = meshPeersByTopic.get(topic.topicID);
        if (mesh != null && !mesh.isEmpty()) return mesh;

        // 2 – fallback: everybody in the topic.members list
        Set<BigInteger> subs = new HashSet<>();
        for (Node n : topic.topicMembers) {
            BigInteger id = ((GossipSubProtocol) n.getProtocol(gossipSubId)).nodeId;
            subs.add(id);
        }
        return subs;
    }
}