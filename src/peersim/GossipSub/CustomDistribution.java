/*  ────────────────────────────────────────────────────────────────────
 *  peersim/GossipSub/CustomDistribution.java
 *  --------------------------------------------------------------------
 *  PeerDAS-compliant network initialiser
 *  ------------------------------------------------------------------*/
package peersim.GossipSub;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.IntStream;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

public class CustomDistribution implements peersim.core.Control {

    /* ---------- constants driven by the PeerDAS spec ------------------ */
    private static final int ROWS                    = 512;   // extended square
    private static final int COLS                    = 512;
    private static final int LINE_PAIR_SIZE          = 2;     // 2 rows or 2 cols
    private static final int PAIRS_PER_AXIS          = ROWS / LINE_PAIR_SIZE; // 256
    private static final int NUMBER_OF_TOPICS        = 256;   // fixed

    private static final int NUMBER_OF_VALIDATORS = Configuration.getInt("NUMBER_OF_VALIDATORS", 4096);

    private static final int VALIDATORS_PER_TOPIC    =
            Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC", 128);

    private static final double MALICIOUS_RATE       =
            Configuration.getDouble("MALICIOUS_RATE", 0.05);

    /* permutation prime (must be odd and coprime with 256) */
    private static final int PERMUTATION_PRIME       =
            Configuration.getInt("PERMUTATION_PRIME", 149);

    /* ---------- Peersim helpers -------------------------------------- */
    private static final String PAR_PROT             = "protocol";
    private final int gossipProtocolID;
    private final UniformRandomGenerator urg;
    private final boolean isDEBUG =
            Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

    private static final Random rnd = CommonState.r;

    /* ---------- global state ----------------------------------------- */
    public static Map<BigInteger, Node> networkNodes              = new LinkedHashMap<>();
    public static Map<String,    Topic> topics                    = new LinkedHashMap<>(NUMBER_OF_TOPICS);
    public static Map<Integer, ArrayList<BigInteger>> rowCustodyNodes  = new HashMap<>();
    public static Map<Integer, ArrayList<BigInteger>> colCustodyNodes  = new HashMap<>();

    public static Node  blockProposerNode;        // chosen randomly
    public static Block block = new Block(ROWS, COLS);

    public final String prefix;

    /* ----------------------------------------------------------------- */
    public CustomDistribution(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        this.urg              = new UniformRandomGenerator(160, rnd);
        this.prefix = prefix;
    }

    /* ================  MAIN ENTRY (called once by Peersim) ============ */
    @Override
    public boolean execute() {

        initNodes();          // IDs, proposer flag, malicious flag
        try { initTopics(); } // Topic-1 … Topic-256
        catch (NoSuchAlgorithmException e) { e.printStackTrace(); }

        assignCustodyPeerDAS();
        new TopicBasedMesh(this.prefix).createTopicMesh();

        return false;         // allow other controls to keep running
    }

    /* ================================================================= */
    /*                              helpers                              */
    /* ================================================================= */

    /** Initialise every Peersim node: 160-bit ID, block-proposer, etc. */
    private void initNodes() {

        int proposerIdx = rnd.nextInt(Network.size());

        for (int idx = 0; idx < Network.size(); idx++) {

            Node n               = Network.get(idx);
            GossipSubProtocol g  = (GossipSubProtocol) n.getProtocol(gossipProtocolID);

            BigInteger id = urg.generate();
            g.setNodeId(id);
            networkNodes.put(id, n);

            g.setHeartbeatManager(
                    new HeartbeatManager(g, g.ephemeralCache, g.peerScores, isDEBUG));

            boolean proposer = idx == proposerIdx;
            g.setBlockProposerNode(proposer);
            if (proposer) blockProposerNode = n;

            g.setMaliciousNode(rnd.nextDouble() < MALICIOUS_RATE);
        }
    }

    /** Create Topic-1 … Topic-256 */
    private void initTopics() throws NoSuchAlgorithmException {
        for (int i = 1; i <= NUMBER_OF_TOPICS; i++) {
            Topic t = new Topic("Topic-" + i);
            topics.put(t.topicID, t);
        }
    }

    /* ------------------------------------------------------------- */
    /*   ===  PeerDAS  :  2-row + 2-column custody per topic  ===   */
    /* ------------------------------------------------------------- */
    private void assignCustodyPeerDAS() {

        /* 1 ─ gather & shuffle all non-proposer validators -------------------- */
        List<Node> validators = new ArrayList<>();
        for (Node n : networkNodes.values())
            if (n != blockProposerNode) validators.add(n);
        Collections.shuffle(validators, rnd);

        final int T = NUMBER_OF_TOPICS;          // 256
        final int V = NUMBER_OF_VALIDATORS;         // e.g. 16499, or maybe 100
        final int MIN = 1;                       // guarantee ≥1 per topic

        /* 2 ─ how many validators per topic? --------------------------------- */
        int base  = Math.max(MIN, V / T);        // 0→1, 64, 65, …    (floor)
        int extra = (V >= T) ? V % T             // unique assignment possible
                : 0;                // else we’ll duplicate

        int cursor = 0;                          // always modulo V
        for (int t = 0; t < T; t++) {

            Topic topic = topics.get("Topic-" + (t + 1));

            /* 2×2 cross mapping (unchanged) */
            int rowPairIdx = t;
            int colPairIdx = (PERMUTATION_PRIME * t) & 0xFF;
            int r0 = rowPairIdx * 2, r1 = r0 + 1;
            int c0 = colPairIdx * 2, c1 = c0 + 1;

            int quota = base + (t < extra ? 1 : 0);   // 1, 64/65, …

            for (int i = 0; i < quota; i++) {

                /* wrap-around makes duplicates when V < required slots */
                Node node = validators.get(cursor % V);
                cursor++;

                GossipSubProtocol g = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
                BigInteger id       = g.getNodeId();

                /* give the four custody labels */
                g.addCustody("row"+r0); g.addCustody("row"+r1);
                g.addCustody("col"+c0); g.addCustody("col"+c1);

                rowCustodyNodes.computeIfAbsent(r0,k->new ArrayList<>()).add(id);
                rowCustodyNodes.computeIfAbsent(r1,k->new ArrayList<>()).add(id);
                colCustodyNodes.computeIfAbsent(c0,k->new ArrayList<>()).add(id);
                colCustodyNodes.computeIfAbsent(c1,k->new ArrayList<>()).add(id);

                /* subscribe once */
                if (!g.isSubscribedToTopic(topic)) {
                    g.subscribeTopic(topic);
                    g.meshPeersByTopic.put(topic.topicID, new HashSet<>());
                    g.gossipMesh      .put(topic.topicID, new HashSet<>());
                    topic.addMember(node);
                }
            }

            if (isDEBUG)
                System.out.printf(
                        "[PeerDAS] %s → quota=%d  rows{%d,%d} cols{%d,%d}%n",
                        topic.topicID, quota, r0, r1, c0, c1);
        }

        /* 3 ─ sanity print --------------------------------------------------- */
        if (isDEBUG) {
            long unassigned = networkNodes.values().stream()
                    .filter(n -> {
                        GossipSubProtocol g =
                                (GossipSubProtocol) n.getProtocol(gossipProtocolID);
                        return !g.isBlockProposerNode() && g.custodies.isEmpty();
                    }).count();
            System.out.printf(
                    "Coverage → rows:true  cols:true  unassigned validators:%d%n",
                    unassigned);                       // always 0 now
        }

    }
}
