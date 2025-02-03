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

import static peersim.GossipSub.CustomDistribution.networkNodes;
import static peersim.GossipSub.CustomDistribution.topics;

public class GossipSubProtocol implements Cloneable, EDProtocol {

    private static final int MESSAGE_CACHE_SIZE = 1024;
    private static final String PAR_TRANSPORT = "transport";

    private static String prefix = null;
    public BigInteger nodeId;

    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;
    private int degreeLow = 6;
    private int degreeHigh = 14;
    protected int degree = 8;

    private Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> localMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> topicNodes = new HashMap<>();

    private LinkedHashMap<Long, Message> messageCache = new LinkedHashMap<>();

    // Statistics
    public IncrementalStats seedArrivalTimeStore = new IncrementalStats();
    public ArrayList<Long> messageArrivalTimeFromBP = new ArrayList<>();
    public ArrayList<Long> messageDelayTimeFromBP = new ArrayList<>();

    public IncrementalStats seedPartArrivalTimeStore = new IncrementalStats();
    public ArrayList<Long> seedPartArrivalTimeFromPeer = new ArrayList<>();
    public ArrayList<Long> seedPartDelayTimeFromPeer = new ArrayList<>();

    public IncrementalStats samplingRTTTimeStore = new IncrementalStats();
    public ArrayList<Long> sampleArrivalTime = new ArrayList<>();
    public ArrayList<Long> sampleDelayTime = new ArrayList<>();

    public int sampleRequestUnsuccessful = 0;
    public int NoOfSampleRequestsSent = 0;
    public int NoOfSamplesRecieved = 0;
    public int NoOfSeedPartsRecieved = 0;

    private int partRequestCounter = 3;
    private int distributionStrategy;

    private ArrayList<byte[][]> custodyData1 = new ArrayList<>();
    private ArrayList<byte[][][]> custodyData2 = new ArrayList<>();

    private int interfaceBandwidth;
    private int blockProducerBandwidth;

    private long lastMessageTransmissionTime;
    private ArrayList<Message> messageQueue = new ArrayList<>();
    private ArrayList<Long> messageTransmissionDelayQueue = new ArrayList<>();

    private TreeMap<Long, Message> sentMsg = new TreeMap<>();
    private TreeMap<Long, Message> sentSeedingPartMsg = new TreeMap<>();

    private int randomSampleCounter;

    public GossipSubProtocol(String prefix) {
        GossipSubProtocol.prefix = prefix;
        this.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
        this.interfaceBandwidth = Configuration.getInt("INTERFACE_BANDWIDTH", 100000000);
        this.blockProducerBandwidth = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1000000000);
        this.distributionStrategy = Configuration.getInt("DISTRIBUTION_STRATEGY");

        Random rnd = new Random();
        this.randomSampleCounter = rnd.nextInt(Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") - 1);

        // Subscribe the node to at least one topic
        if (!topics.isEmpty()) {
            String firstTopic = topics.keySet().iterator().next();
            subscribeTopic(topics.get(firstTopic));
            System.out.println("[DEBUG] Node " + this.nodeId + " automatically subscribed to topic " + firstTopic);
        }
    }

    public Object clone() {
        return new GossipSubProtocol(GossipSubProtocol.prefix);
    }

    public void setNodeId(BigInteger nodeId) {
        if (nodeId == null) {
            System.err.println("[ERROR] Attempting to set a null nodeId!");
            return;
        }

        this.nodeId = nodeId;
        // System.out.println("[DEBUG] Node ID set: " + nodeId);
    }

    public BigInteger getNodeId() {
        return nodeId;
    }

    public void setTopicMembersList(String ID, Set<BigInteger> members) {
        String topicKey = "Topic-" + ID;

        if (!topics.containsKey(topicKey)) {
            System.err.println("[ERROR] Topic " + topicKey + " does not exist. Creating it...");
            topics.put(topicKey, new Topic(topicKey)); // Create the topic if missing
        }

        Topic topic = topics.get(topicKey);
        topic.topicMembers.clear(); // Clear existing members

        for (BigInteger nodeId : members) {
            if (networkNodes.containsKey(nodeId)) {
                topic.addMember(networkNodes.get(nodeId)); // Add node to topic
            }
        }

        topicNodes.put(topicKey, new HashSet<>(members)); // Update local mapping
    }

    private int calculateMessageSize(Message message) {
        int size = Integer.BYTES + Long.BYTES;
        size += message.src.toString().getBytes().length;
        size += message.dest.toString().getBytes().length;

        if (message.messageTopicID != null)
            size += message.messageTopicID.getBytes().length;
        if (message.body != null) {
            if (message.body instanceof byte[][]) {
                for (byte[] row : (byte[][]) message.body)
                    size += row.length;
            } else if (message.body instanceof String) {
                size += ((String) message.body).getBytes().length;
            } else if (message.body instanceof byte[][][]) {
                for (byte[][] matrix : (byte[][][]) message.body) {
                    for (byte[] row : matrix)
                        size += row.length;
                }
            }
        }

        size += 1 + Integer.BYTES * 2 + Long.BYTES;
        return size;
    }

    public void subscribeTopic(Topic topic) {
        if (!subscribedTopics.contains(topic)) {
            subscribedTopics.add(topic);

            // Subcribed as expected
            System.out.println("[DEBUG] Node " + nodeId + " subscribed to topic " + topic.topicID);
        }
    }

    public void unsubscribeTopic(Topic topic) {
        if (subscribedTopics.remove(topic)) {
            localMesh.remove(topic.topicID);
        }
    }

    public void publishMessage(Message m, BigInteger destId, int myPid) {
        System.out.println("[DEBUG] Checking if destination exists: " + destId);

        if (!CustomDistribution.networkNodes.containsKey(destId)) {
            System.err.println("[ERROR] Destination node " + destId + " not found in networkNodes.");
            return;
        }

        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(destId);
        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        long latency = transport.getLatency(src, dest);

        int messageSize = calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / interfaceBandwidth);
        long totalDelay = transmissionDelay + latency;

        System.out.println("[DEBUG] Sending message ID: " + m.id + " from " + this.nodeId + " to " + destId +
                " with delay " + totalDelay);

        // Check if message is added to the event-driven simulator
        System.out.println("[DEBUG] Scheduling message ID: " + m.id + " to be delivered at time: "
                + (CommonState.getTime() + totalDelay));

        EDSimulator.add(totalDelay, m, dest, myPid);
        System.out.println("[DEBUG] Scheduling event for message ID: " + m.id + " at node: " + destId + " with delay: "
                + totalDelay);

    }

    public void handleIHave(Message m, int myPid) {
        if (!messageCache.containsKey(m.id)) {
            System.out.println("[DEBUG] Node " + this.nodeId + " received IHAVE for message ID: " + m.id);

            Message request = createMessage(m.id, Message.MSG_IWANT, m.src, m.messageTopicID, "",
                    m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.ackId);

            publishMessage(request, m.src, myPid);
            System.out.println("[DEBUG] IWANT request sent for message ID: " + m.id);
            NoOfSampleRequestsSent++;
        }
    }

    public void handleIWANT(Message m, int myPid) {
        if (messageCache.containsKey(m.ackId)) {
            Message response = createMessage(m.id, Message.MSG_DATA, m.src, m.messageTopicID, "",
                    m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.ackId);
            publishMessage(response, m.src, myPid);
        }
    }

    public void handleBlockProducerData(Message m, int myPid) {
        if (!messageCache.containsKey(m.id)) {
            messageCache.put(m.id, m);
            messageArrivalTimeFromBP.add(CommonState.getTime());
            messageDelayTimeFromBP.add(CommonState.getTime() - m.timestamp);
            seedArrivalTimeStore.add(CommonState.getTime() - m.timestamp);

            // test
            System.out.println("[DEBUG] handleBlockProducerData() - Processing message ID: " + m.id);
        }
    }

    public void handleData(Message m, int myPid) {
        System.out.println("[DEBUG] Node " + this.nodeId + " received MSG_DATA, Message ID: " + m.id +
                " from " + m.src + " at time " + CommonState.getTime());

        if (!messageCache.containsKey(m.id)) {
            messageCache.put(m.id, m);

            if (sentSeedingPartMsg.containsKey(m.id)) {
                System.out.println("[DEBUG] Node " + this.nodeId + " received expected seed part ID: " + m.id);

                seedPartArrivalTimeFromPeer.add(CommonState.getTime());
                seedPartDelayTimeFromPeer.add(CommonState.getTime() - m.timestamp);
                seedPartArrivalTimeStore.add(CommonState.getTime() - m.timestamp);
                NoOfSeedPartsRecieved++;
                sentSeedingPartMsg.remove(m.id);
                custodyData2.add((byte[][][]) m.body);
                partRequestCounter--;
            }
        } else {
            System.out.println("[DEBUG] Node " + this.nodeId + " already has message ID: " + m.id);
        }
    }

    public void handleSampleRequest(Message m, int myPid) {
        boolean requestedRow = m.isRow;
        int rowOrColNumb = m.rowOrColumnNumber;
        int idx = Integer.parseInt((String) m.body);

        for (Map.Entry<Long, Message> msg : messageCache.entrySet()) {
            Message curMessage = msg.getValue();
            // test
            System.out.println("[DEBUG] Received handle sample request: " + m.id);

            if (curMessage.isRow == m.isRow && curMessage.rowOrColumnNumber == m.rowOrColumnNumber) {
                byte[][] data = (byte[][]) curMessage.body;
                byte sampleDataResp = data[0][idx];
                Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, m.src,
                        m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber,
                        m.partNumber, m.timestamp, m.ackId);
                publishMessage(sampleResponse, m.src, gossipSubId);
                return;
            }
        }

    }

    public void handleSampleResponse(Message m, int myPid) {
        if (!sentMsg.containsKey(m.id)) {
            System.err.println("[ERROR] Received MSG_SAMPLE_DATA_RESPONSE for unknown message ID: " + m.id);
            return;
        }

        sentMsg.remove(m.id);
        sampleArrivalTime.add(CommonState.getTime());
        sampleDelayTime.add(CommonState.getTime() - m.timestamp);
        samplingRTTTimeStore.add(CommonState.getTime());
        NoOfSamplesRecieved++;
    }

    public void handleTimeout(Timeout t, int myPid) {
        if (sentMsg.containsKey(t.msgID)) {
            sampleRequestUnsuccessful++;
            NoOfSampleRequestsSent++;
            Message sampleMsgSent = sentMsg.remove(t.msgID);
            Message msgToResend = createMessage(-1, sampleMsgSent.getType(), sampleMsgSent.dest,
                    sampleMsgSent.messageTopicID, sampleMsgSent.body, sampleMsgSent.isRow,
                    sampleMsgSent.rowOrColumnNumber, sampleMsgSent.partNumber, 0, sampleMsgSent.ackId);
            sentMsg.put(msgToResend.id, msgToResend);
            publishMessage(msgToResend, msgToResend.dest, myPid);
        }
    }

    public void handleBandwidthReset(Message m, int myPid) {
        this.messageQueue.clear();
        this.messageTransmissionDelayQueue.clear();
        this.lastMessageTransmissionTime = 0;
        System.out.println("Bandwidth reset at node: " + this.nodeId + " at time " + CommonState.getTime());
    }

    private Message createMessage(long id, int type, BigInteger dest, String topicID,
            Object body, boolean isRow, int rowOrColNum,
            int partNum, long timeStamp, long ackid) {

        long uniqueId = (id == -1) ? CommonState.r.nextLong() : id; // Generate unique ID if not provided

        Message msg = new Message(uniqueId, type, isRow, rowOrColNum, partNum, ackid);
        msg.timestamp = (id == -1) ? CommonState.getTime() : timeStamp;
        msg.src = (this.nodeId != null) ? this.nodeId : BigInteger.ZERO; // Prevents null values
        msg.dest = dest;
        msg.messageTopicID = topicID;
        msg.body = body;

        System.out.println("[DEBUG] Created message ID: " + msg.id +
                " Type: " + type +
                " From: " + msg.src +
                " To: " + msg.dest);
        return msg;
    }

    private void blockProducer() {
        int rowNumber = 0;
        int columnNumber = 0;
        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS"), null);
        GossipSubProtocol blockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));

        System.out.println("[INFO] Block Proposer ID: " + blockProposer.getNodeId());

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            int nodeCounter = 0;

            for (Node n : topicEntry.getValue().topicMembers) {
                BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                byte[][] rowToSend = b.getRowData(rowNumber);
                sendBlockData(destID, topicEntry.getValue().topicID, rowToSend, true, rowNumber);

                // Ensure message is stored in cache
                Message newMessage = new Message(Message.MSG_DATA, rowToSend, true, rowNumber, -1, -1);
                messageCache.put(newMessage.id, newMessage);

                nodeCounter++;
                if (nodeCounter % 8 == 0)
                    rowNumber++;
            }
        }
        System.out.println("[INFO] Block proposer has sent all messages.");
    }

    private void shardingBasedDistribution() {
        int numberOfDivisions = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2;
        int partSize = Configuration.getInt("NUMBER_OF_ROWS") / numberOfDivisions;

        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS"), null);
        GossipSubProtocol blockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));

        System.out.println("[INFO] Block Proposer ID: " + blockProposer.getNodeId());

        int topicIndex = 0;
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            int rowPartNo = 0, colPartNo = 0;
            int nodeCounter = 0;

            for (Node n : topicEntry.getValue().topicMembers) {
                BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();

                if (nodeCounter < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2) {
                    byte[][][] rowParts = createBlockParts(b, topicIndex, rowPartNo, partSize, numberOfDivisions, true);

                    if (rowParts == null) {
                        System.err.println("[ERROR] RowParts is NULL for topic: " + topicEntry.getValue().topicID);
                    }

                    sendShardedBlockData(destID, topicEntry.getValue().topicID, rowParts, true, rowPartNo);
                    nodeCounter++;
                    if (nodeCounter % 8 == 0)
                        rowPartNo++;
                } else {
                    byte[][][] colParts = createBlockParts(b, topicIndex, colPartNo, partSize, numberOfDivisions,
                            false);

                    if (colParts == null) {
                        System.err.println("[ERROR] ColumnParts is NULL for topic: " + topicEntry.getValue().topicID);
                    }

                    sendShardedBlockData(destID, topicEntry.getValue().topicID, colParts, false, colPartNo);
                    nodeCounter++;
                    if (nodeCounter % 8 == 0)
                        colPartNo++;
                }
            }
            topicIndex++;
        }
        System.out.println("[INFO] Sharding-based block proposer has sent all messages.");
    }

    private void handleBlockProposer(int myPid) {
        System.out.println("[INFO] Node " + this.nodeId + " is the block proposer.");

        if (this.nodeId
                .equals(((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId)) {
            System.out.println("[INFO] Starting message distribution...");

            if (distributionStrategy == 1) {
                blockProducer();
            } else if (distributionStrategy == 2) {
                shardingBasedDistribution();
            } else {
                System.err.println("[ERROR] Unknown distribution strategy: " + distributionStrategy);
            }
        } else {
            System.err.println("[ERROR] Received MSG_BLOCK_PROPOSER, but this node is not the proposer.");
        }
    }

    /**
     * Sends block data (row/column) to the destination node.
     */
    private void sendBlockData(BigInteger destID, String topicID, byte[][] data, boolean isRow, int index) {
        System.out
                .println("[DEBUG] Sending block data from " + this.nodeId + " to " + destID + " for topic " + topicID);
        Message newMessage = new Message(Message.MSG_DATA, data, isRow, index, -1, -1);
        newMessage.src = this.getNodeId();
        newMessage.dest = destID;
        newMessage.messageTopicID = topicID;

        publishMessage(newMessage, destID, gossipSubId);
    }

    /**
     * Sends sharded block data (row/column parts) to the destination node.
     */
    private void sendShardedBlockData(BigInteger destID, String topicID, byte[][][] data, boolean isRow,
            int partIndex) {
        if (data == null) {
            System.err.println("[ERROR] Data is NULL in sendShardedBlockData()!");
            return;
        }

        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                System.err.println("[ERROR] data[" + i + "] is NULL!");
            }
            for (int j = 0; j < data[i].length; j++) {
                if (data[i][j] == null) {
                    System.err.println("[ERROR] data[" + i + "][" + j + "] is NULL!");
                }
            }
        }

        Message newMessage = new Message(Message.MSG_DATA, data, isRow, -1, partIndex, -1);
        newMessage.src = this.getNodeId();
        newMessage.dest = destID;
        newMessage.messageTopicID = topicID;

        publishMessage(newMessage, destID, gossipSubId);
    }

    /**
     * Creates sharded parts of a block (rows or columns).
     */
    private byte[][][] createBlockParts(Block b, int topicIndex, int partNo, int partSize, int numDivisions,
            boolean isRow) {
        byte[][][] parts = new byte[numDivisions][][]; // Ensure exact number of divisions
        int totalSize = 512; // Maximum allowed size for rows/columns

        int start = partNo * partSize;
        int end = Math.min(start + partSize, totalSize); // Ensure `end` never exceeds 512

        // Prevent invalid partitions
        if (start >= totalSize) {
            System.err.println("[ERROR] Invalid partition: start=" + start + " exceeds limit of " + totalSize);
            return new byte[0][][]; // Return empty to prevent crashes
        }

        if (start >= end) {
            System.err.println("[ERROR] Invalid range in createBlockParts: start=" + start + " end=" + end);
            return new byte[0][][]; // Prevent invalid copy range
        }

        for (int i = 0; i < numDivisions; i++) {
            try {
                byte[][] data = isRow ? b.getRowData(topicIndex * numDivisions + i)
                        : b.getColumnData(topicIndex * numDivisions + i);

                if (data == null || data.length < totalSize) {
                    System.err.println("[ERROR] Data is null or too small for topicIndex=" + topicIndex + " i=" + i);
                    continue;
                }

                parts[i] = Arrays.copyOfRange(data, start, end);
            } catch (Exception e) {
                System.err.println("[ERROR] Exception in createBlockParts: " + e.getMessage());
            }
        }
        return parts;
    }

    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;

        System.out.println("[DEBUG] Processing event type: " + event.getClass().getSimpleName());

        if (event instanceof Message) {
            Message m = (Message) event;

            System.out.println("[DEBUG] Message received - ID: " + m.id + " Type: " + m.getType() + " From: " + m.src
                    + " To: " + m.dest);

            switch (m.getType()) {
                case Message.MSG_IHAVE:
                    System.out.println("[DEBUG] Handling MSG_IHAVE for message ID: " + m.id);
                    handleIHave(m, myPid);
                    break;

                case Message.MSG_IWANT:
                    System.out.println("[DEBUG] Handling MSG_IWANT for message ID: " + m.id);
                    handleIWANT(m, myPid);
                    break;

                case Message.MSG_DATA:
                    System.out.println("[DEBUG] Handling MSG_DATA for message ID: " + m.id);
                    if (m.src == null) {
                        System.err.println("[ERROR] Received MSG_DATA with null source! Message ID: " + m.id);
                        return;
                    }
                    if (m.src.equals(((GossipSubProtocol) (CustomDistribution.blockProposerNode
                            .getProtocol(gossipSubId))).nodeId)) {
                        System.out.println("[DEBUG] This is a block producer data message.");
                        handleBlockProducerData(m, myPid);
                    } else {
                        handleData(m, myPid);
                    }
                    break;

                case Message.MSG_BLOCK_PROPOSER:
                    System.out.println("[DEBUG] Handling MSG_BLOCK_PROPOSER for message ID: " + m.id);
                    handleBlockProposer(myPid);
                    break;

                case Message.MSG_SAMPLE_DATA_REQUEST:
                    System.out.println("[DEBUG] Handling MSG_SAMPLE_DATA_REQUEST for message ID: " + m.id);
                    handleSampleRequest(m, myPid);
                    break;

                case Message.MSG_SAMPLE_DATA_RESPONSE:
                    System.out.println("[DEBUG] Handling MSG_SAMPLE_DATA_RESPONSE for message ID: " + m.id);
                    if (sentMsg.containsKey(m.id)) {
                        sentMsg.remove(m.id);
                        handleSampleResponse(m, myPid);
                    }
                    break;

                default:
                    System.err.println("[ERROR] Unknown Message type: " + m.getType());
                    break;
            }

        } else {
            System.err.println("[ERROR] Unknown event type: " + event.getClass().getName());
        }
    }

}
