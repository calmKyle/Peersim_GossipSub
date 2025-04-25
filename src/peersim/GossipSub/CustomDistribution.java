package peersim.GossipSub;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

/**
 * Initializes each node with a unique random NodeId.
 * The block proposer node is chosen at random.
 */
public class CustomDistribution implements peersim.core.Control {

    private static final String PAR_PROT = "protocol";

    private static final int NUMBER_OF_TOPICS = Configuration.getInt("NUMBER_OF_TOPICS", 64);
    private static final int NUMBER_OF_VALIDATOR_NODES = Configuration.getInt("NUMBER_OF_VALIDATORS", 8192);
    private static final int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC",
            16);
    private static final int NUMBER_OF_VALIDATORS_PER_TOPIC = Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC",
            128);

    // Number of row or column holders per "segment"
    private static final int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC = (NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1)
            ? NUMBER_OF_VALIDATORS_PER_TOPIC
            : (int) Math.ceil(
            (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) /
                    Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));

    // Malicious Rate
    private static final double MALICIOUS_RATE =
            Configuration.getDouble("MALICIOUS_RATE", 0.05);

    private final int gossipProtocolID;
    private final UniformRandomGenerator urg;
//    private static final Random random = new Random();

    private static final Random random = CommonState.r;
    // blockProposerNode will be assigned at random now
    public static Node blockProposerNode;

    // Custody maps for row/column
    public static HashMap<Integer, ArrayList<BigInteger>> rowCustodyNodes = new HashMap<>();
    public static HashMap<Integer, ArrayList<BigInteger>> columnCustodyNodes = new HashMap<>();

    public static Block block = new Block(
            Configuration.getInt("NUMBER_OF_ROWS"),
            Configuration.getInt("NUMBER_OF_COLUMNS"));

    public static Map<BigInteger, Node> networkNodes = new LinkedHashMap<>();
    public static Map<String, Topic> topics = new LinkedHashMap<>(NUMBER_OF_TOPICS);

    public final String prefix;
    private boolean isDEBUG = Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

    public CustomDistribution(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
        this.urg = new UniformRandomGenerator(160, CommonState.r);
        this.prefix = prefix;
    }

    @Override
    public boolean execute() {
        System.out.println("NUMBER_OF_ROWSCOLS_IN_A_TOPIC: " + NUMBER_OF_ROWSCOLS_IN_A_TOPIC);
        System.out.println("NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC: " + NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC);
        System.out.println("NUMBER_OF_VALIDATORS_PER_TOPIC: " + NUMBER_OF_VALIDATORS_PER_TOPIC);

        // [ADDED] Randomly pick one index to be block proposer
        int randomIndex = CommonState.r.nextInt(Network.size());
        System.out.println("[CustomDistribution] Choosing node index " + randomIndex + " as block proposer");

        // 1. Assign NodeIDs and pick the random node as block proposer
        for (int i = 0; i < Network.size(); i++) {
            BigInteger nodeId = urg.generate();
            Node n = Network.get(i);

            GossipSubProtocol gsp = (GossipSubProtocol) n.getProtocol(gossipProtocolID);
            gsp.setNodeId(nodeId);

            // Each node also needs a HeartbeatManager
            HeartbeatManager hm = new HeartbeatManager(
                    gsp,
                    gsp.ephemeralCache,
                    gsp.peerScores,
                    isDEBUG);
            gsp.setHeartbeatManager(hm);

            networkNodes.put(nodeId, n);

            // If it's our chosen random index, mark as block proposer
            if (i == randomIndex) {
                blockProposerNode = n;
                gsp.setBlockProposerNode(true);
                System.out.println("[CustomDistribution] Node " + i + " / NodeID=" + nodeId + " is BLOCK PROPOSER");
            } else {
                gsp.setBlockProposerNode(false);
            }

            // Mark for malicious
            double randVal = CommonState.r.nextDouble();
            if (randVal < MALICIOUS_RATE) {
                gsp.setMaliciousNode(true);
                System.out.println("[CustomDistribution] Node " + i + " / ID=" + nodeId
                        + " is MALICIOUS!");
            } else {
                gsp.setMaliciousNode(false);
            }
        }

        // 2. Initialize topics and assign custody
        try {
            initTopics();
            assignCustodyWithChunks();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return false;
    }

    public Map<BigInteger, Node> getNetworkNodes() {
        return networkNodes;
    }

    /**
     * Creates all required Topic objects and stores them in `topics`.
     */
    private void initTopics() throws NoSuchAlgorithmException {
        System.out.println("Initializing topics...");

        for (int i = 1; i <= NUMBER_OF_TOPICS; i++) {
            Topic topic = new Topic("Topic-" + i);
            topics.put(topic.topicID, topic);
        }
    }

    /**
     * Assign row and column custody to validator nodes in chunks.
     */
    private void assignCustodyWithChunks() {
        System.out.println("Assigning row/column custody in chunks...");

        // 1. Build a validator list that excludes the block proposer
        List<Node> validatorNodes = new ArrayList<>();
        for (Node n : networkNodes.values()) {
            if (n == blockProposerNode) {
                continue;
            }
            if (validatorNodes.size() >= NUMBER_OF_VALIDATOR_NODES) {
                break;
            }
            validatorNodes.add(n);
        }

        // 2. Initialize row/column counters and topic counters
        int rowNumber = 0;
        int columnNumber = 0;
        rowCustodyNodes.put(rowNumber, new ArrayList<>());
        columnCustodyNodes.put(columnNumber, new ArrayList<>());

        int topicNumber = 1; // Start from topic 1
        int assignedInCurrentTopic = 0; // how many nodes assigned so far in the current topic

        // 3. Iterate through validatorNodes in a loop
        int i = 0;
        while (i < validatorNodes.size()) {
            // If assigned enough for the current topic, move to next
            if (assignedInCurrentTopic >= NUMBER_OF_VALIDATORS_PER_TOPIC) {
                topicNumber++;
                assignedInCurrentTopic = 0;
            }

            // ROW CHUNK
            ChunkResult rowResult = assignCustodyChunk(
                    validatorNodes,
                    i,
                    NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC,
                    topicNumber,
                    assignedInCurrentTopic,
                    rowNumber,
                    "row");
            i = rowResult.nextNodeIndex;
            assignedInCurrentTopic = rowResult.assignedInThisTopic;
            rowNumber = rowResult.nextLabelNumber;

            if (i >= validatorNodes.size()) {
                break;
            }
            if (assignedInCurrentTopic >= NUMBER_OF_VALIDATORS_PER_TOPIC) {
                topicNumber++;
                assignedInCurrentTopic = 0;
            }

            // COLUMN CHUNK
            ChunkResult colResult = assignCustodyChunk(
                    validatorNodes,
                    i,
                    NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC,
                    topicNumber,
                    assignedInCurrentTopic,
                    columnNumber,
                    "column");
            i = colResult.nextNodeIndex;
            assignedInCurrentTopic = colResult.assignedInThisTopic;
            columnNumber = colResult.nextLabelNumber;
        }

        // 4. Create the mesh in each topic
        TopicBasedMesh tbm = new TopicBasedMesh(this.prefix);
        tbm.createTopicMesh();

        // 5. AFTER all custody assignments are done, verify
        for (Map.Entry<BigInteger, Node> entry : networkNodes.entrySet()) {
            GossipSubProtocol gsp = (GossipSubProtocol) entry.getValue().getProtocol(gossipProtocolID);
            String c1 = gsp.custody1;
            String c2 = gsp.custody2;

            if (c1 == null || c2 == null || c1.isEmpty() || c2.isEmpty()) {
                System.out.println("[CUSTODY WARNING] Node " + gsp.getNodeId()
                        + " has incomplete custody: c1=" + c1 + ", c2=" + c2);
            }
        }
    }

    private static class ChunkResult {
        int nextNodeIndex;
        int assignedInThisTopic;
        int nextLabelNumber;

        ChunkResult(int nextNodeIndex, int assignedInThisTopic, int nextLabelNumber) {
            this.nextNodeIndex = nextNodeIndex;
            this.assignedInThisTopic = assignedInThisTopic;
            this.nextLabelNumber = nextLabelNumber;
        }
    }

    private ChunkResult assignCustodyChunk(
            List<Node> validatorNodes,
            int startIndex,
            int chunkSize,
            int topicNumber,
            int assignedSoFar,
            int labelNumber,
            String labelPrefix) {
        int i = startIndex;
        int assignedInTopic = assignedSoFar;

        for (int count = 0; count < chunkSize; count++) {
            if (i >= validatorNodes.size()) break;
            if (assignedInTopic >= NUMBER_OF_VALIDATORS_PER_TOPIC) break;

            Node node = validatorNodes.get(i++);
            GossipSubProtocol gsp = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
            BigInteger nodeId = gsp.getNodeId();

            // Primary custody
            gsp.custody1 = labelPrefix + labelNumber;
            if (labelPrefix.equals("row")) {
                rowCustodyNodes.computeIfAbsent(labelNumber, k -> new ArrayList<>()).add(nodeId);
            } else {
                columnCustodyNodes.computeIfAbsent(labelNumber, k -> new ArrayList<>()).add(nodeId);
            }

            // Secondary custody
            int dimension = Configuration.getInt("NUMBER_OF_ROWS");
            int randomX;
            do {
                randomX = random.nextInt(dimension);
            } while (randomX == labelNumber);

            gsp.custody2 = labelPrefix + randomX;
            if (labelPrefix.equals("row")) {
                rowCustodyNodes.computeIfAbsent(randomX, k -> new ArrayList<>()).add(nodeId);
            } else {
                columnCustodyNodes.computeIfAbsent(randomX, k -> new ArrayList<>()).add(nodeId);
            }

            // Subscribe to the primary topic
            subscribeNodeToTopic(gsp, node, topicNumber);

            // Subscribe to a secondary topic determined by randomX
            int secondaryTopicNumber = (randomX / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC")) + 1;
            if (NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1) {
                if (labelPrefix.equals("row")) {
                    secondaryTopicNumber = (randomX * 2) + 1;
                } else {
                    secondaryTopicNumber = (randomX * 2) + 2;
                }
                if (secondaryTopicNumber > NUMBER_OF_TOPICS) {
                    secondaryTopicNumber = secondaryTopicNumber % NUMBER_OF_TOPICS;
                    if (secondaryTopicNumber == 0) {
                        secondaryTopicNumber = NUMBER_OF_TOPICS;
                    }
                }
            }
            subscribeNodeToTopic(gsp, node, secondaryTopicNumber);

            assignedInTopic++;

            // If assigned chunkSize nodes in this row/col, move to next labelNumber
            if (assignedInTopic % chunkSize == 0) {
                labelNumber++;
                if (labelPrefix.equals("row")) {
                    rowCustodyNodes.putIfAbsent(labelNumber, new ArrayList<>());
                } else {
                    columnCustodyNodes.putIfAbsent(labelNumber, new ArrayList<>());
                }
            }

            if (isDEBUG) {
                System.out.println(
                        "[CHUNK " + labelPrefix.toUpperCase() + "] Node=" + nodeId
                                + ", T1=" + topicNumber + ", T2=" + secondaryTopicNumber
                                + ", C1=" + gsp.custody1 + ", C2=" + gsp.custody2);
            }
        }

        return new ChunkResult(i, assignedInTopic, labelNumber);
    }

    private void subscribeNodeToTopic(GossipSubProtocol gsp, Node node, int topicNumber) {
        Topic topic = topics.get("Topic-" + topicNumber);
        if (topic == null) {
            return;
        }
        if (!gsp.isSubscribedToTopic(topic)) {
            gsp.subscribeTopic(topic);
            gsp.localMesh.put(topic.topicID, new HashSet<>());
            gsp.gossipMesh.put(topic.topicID, new HashSet<>());
            topic.addMember(node);

            if (isDEBUG) {
                System.out.println(
                        "[SUBSCRIBE] Node " + gsp.getNodeId() + " -> Topic-" + topicNumber);
            }
        }
    }
}
