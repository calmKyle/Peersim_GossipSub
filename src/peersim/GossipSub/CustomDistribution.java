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
 * The first node becomes the block proposer.
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

    private final int gossipProtocolID;
    private final UniformRandomGenerator urg;
    private static final Random random = new Random();

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
    // private boolean isDEBUG = true;

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

        // 1. Assign NodeIDs and pick the first node as block proposer
        for (int i = 0; i < Network.size(); i++) {
            BigInteger nodeId = urg.generate();
            Node n = Network.get(i);

            GossipSubProtocol gsp = (GossipSubProtocol) n.getProtocol(gossipProtocolID);
            gsp.setNodeId(nodeId);

            // Each node also needs a HeartbeatManager, which references ephemeralCache +
            // peerScores
            HeartbeatManager hm = new HeartbeatManager(
                    gsp,
                    gsp.ephemeralCache, // make sure these are accessible
                    gsp.peerScores,
                    isDEBUG);
            gsp.setHeartbeatManager(hm);

            networkNodes.put(nodeId, n);

            if (i == 0) {
                blockProposerNode = n;
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
     * Assign row and column custody to validator nodes in chunks, using
     * a shared helper method for the row-chunk and column-chunk.
     * iterate through the validator list in "chunks":
     * - First chunk of size NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC => row
     * custody
     * - Second chunk of size NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC => column
     * custody
     * -> Then move to the next set of nodes. Meanwhile, if hit the limit in a
     * topic
     * -> (NUMBER_OF_VALIDATORS_PER_TOPIC), move to the next topic.
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

        // 3. Iterate through validatorNodes in a loop,
        // alternating between "row chunk" and "column chunk."
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

            // if are out of nodes, break
            if (i >= validatorNodes.size()) {
                break;
            }

            // or if the topic was just maxed out by the row chunk
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

        // 5. AFTER all custody assignments are done, verify and print
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

    /**
     * A helper class to store the updated counters after assigning one "chunk."
     * This is returned from assignCustodyChunk() so the calling method knows
     * the new index, how many were assigned in this topic, and the next row/col
     * number.
     */
    private static class ChunkResult {
        int nextNodeIndex; // the updated i
        int assignedInThisTopic; // how many assigned in the current topic so far
        int nextLabelNumber; // the updated rowNumber or columnNumber

        ChunkResult(int nextNodeIndex, int assignedInThisTopic, int nextLabelNumber) {
            this.nextNodeIndex = nextNodeIndex;
            this.assignedInThisTopic = assignedInThisTopic;
            this.nextLabelNumber = nextLabelNumber;
        }
    }

    /**
     * Assigns custody to a "chunk" of nodes (either ROW or COLUMN), updating
     * the relevant counters. The loop stops if run out of nodes or fill the
     * topic.
     *
     * @param validatorNodes All validator nodes
     * @param startIndex     Where in validatorNodes to start
     * @param chunkSize      How many nodes to assign in this chunk
     * @param topicNumber    Current topic
     * @param assignedSoFar  How many nodes assigned so far in the current topic
     * @param labelNumber    rowNumber or columnNumber
     * @param labelPrefix    "row" or "column"
     * @return A ChunkResult with updated i, assignedSoFar, labelNumber
     */
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
            // If have used all nodes, or filled the topic, exit early
            if (i >= validatorNodes.size())
                break;
            if (assignedInTopic >= NUMBER_OF_VALIDATORS_PER_TOPIC)
                break;

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

            // Assign secondary custody with a random different row or column
            int dimension = Configuration.getInt("NUMBER_OF_ROWS"); // if columns use same dimension
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

                // Again, clamp or mod the result:
                if (secondaryTopicNumber > NUMBER_OF_TOPICS) {
                    secondaryTopicNumber = secondaryTopicNumber % NUMBER_OF_TOPICS;
                    if (secondaryTopicNumber == 0) {
                        secondaryTopicNumber = NUMBER_OF_TOPICS;
                    }
                }
            }
            subscribeNodeToTopic(gsp, node, secondaryTopicNumber);

            assignedInTopic++;

            // If assigned chunkSize nodes in this row/col, move to the next
            // labelNumber
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

                for (Map.Entry<BigInteger, Node> entry : networkNodes.entrySet()) {
                    // GossipSubProtocol gsp = (GossipSubProtocol)
                    // entry.getValue().getProtocol(gossipProtocolID);
                    if (gsp.custody1 == null || gsp.custody2 == null
                            || gsp.custody1.isEmpty() || gsp.custody2.isEmpty()) {
                        System.out.println("[CUSTODY WARNING] Node "
                                + gsp.getNodeId() + " only got one seed: "
                                + "c1=" + gsp.custody1 + ", c2=" + gsp.custody2);
                    }
                }

            }
        }

        // Return updated counters
        return new ChunkResult(i, assignedInTopic, labelNumber);
    }

    /**
     * Subscribes a node/gsp to a topic if not already subscribed.
     */
    private void subscribeNodeToTopic(GossipSubProtocol gsp, Node node, int topicNumber) {
        Topic topic = topics.get("Topic-" + topicNumber);
        if (topic == null) {
            return; // in case exceed or mismatch topic indexing
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
