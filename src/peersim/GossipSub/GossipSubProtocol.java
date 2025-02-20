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
import java.util.stream.IntStream;

import static peersim.GossipSub.CustomDistribution.*;

public class GossipSubProtocol implements Cloneable, EDProtocol {

    // Configuration Constants
    private static int MESSAGE_CACHE_SIZE = 1024;
    private static String PAR_TRANSPORT = "transport";
    private static int NUMBER_OF_ROWSCOLS_IN_A_TOPIC = Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
    private static int NUMBER_OF_VALIDATORS_PER_TOPIC = Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC", 128);
    private static int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC = NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1
            ? NUMBER_OF_VALIDATORS_PER_TOPIC
            : (int) Math.ceil(
                    (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0) / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));

    private static String prefix;

    // Instance Variables
    public BigInteger nodeId;
    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;
    private int degreeLow = 6;
    private int degreeHigh = 14;
    protected int degree = 8;

    private Set<Topic> subscribedTopics = new HashSet<>();
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

    public String custody1;
    public String custody2;
    private boolean samplingStarted = false;

    protected static List<Set<BigInteger>> rowColHolders = new ArrayList<>(1024);

    public GossipSubProtocol(String prefix) {
        GossipSubProtocol.prefix = prefix;
        this.nodeId = null;
        this.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
        this.samplingStarted = false;
    }

    public Object clone() {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        return cln;
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
        if (subscribedTopics.contains(topic)) {
            return true;
        }
        return false;
    }

    public void setTopicMembersList(String topicName, Set<BigInteger> members) {
        topicNodes.put(topicName, members);
    }

    public void publishMessage(Message m, BigInteger destId, int myPid) {
        // Determine bandwidth based on whether this node is the block proposer
        int bandwidth = (this.nodeId == ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId))).nodeId) ? blockProducerBandwidth : interfaceBandwidth;

        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        long latency = transport.getLatency(src, dest);

        // Process message queue and update queuing delay
        while (!messageTransmissionDelayQueue.isEmpty()
                && CommonState.getTime() > messageTransmissionDelayQueue.get(0)) {
            messageQueue.remove(0);
            messageTransmissionDelayQueue.remove(0);
        }

        long queuingDelay = messageQueue.isEmpty() ? 0
                : messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1) - CommonState.getTime();

        int messageSize = calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / bandwidth);
        long propagationDelay = latency;
        long totalDelay = queuingDelay + transmissionDelay + propagationDelay;

        // Update transmission statistics
        totalDataTransmitted += messageSize;
        totalTransmissionTime += totalDelay;

        // Schedule message transmission
        EDSimulator.add(totalDelay, m, dest, myPid);

        // Update message queue with the new transmission
        long scheduledTransmissionTime = CommonState.getTime() + transmissionDelay;
        messageQueue.add(m);
        messageTransmissionDelayQueue.add(scheduledTransmissionTime);
        lastMessageTransmissionTime = scheduledTransmissionTime;
    }

    public void sendMessageToTopicNodes(Message m, int myPid, String topicID, Map<String, Set<BigInteger>> nodesInTopic,
            BigInteger messageSender, BigInteger src) {
        Set<BigInteger> topicNodes = nodesInTopic.get(topicID);
        if (topicNodes == null)
            return; // Prevent NullPointerException

        for (BigInteger peerId : topicNodes) {
            if (peerId.equals(messageSender) || peerId.equals(m.src)) {
                continue;
            }
            Message newMessage = createMessage(
                    m.id, m.type, src, peerId, m.messageTopicID, m.body,
                    m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(),
                    (m.ackId == -6) ? m.ackId : m.id);
            publishMessage(newMessage, peerId, myPid);
        }
    }

    public void sendMessageToPeers(Message m, int myPid, String topicID, BigInteger avoid, BigInteger src) {
        Set<BigInteger> peers = localMesh.get(topicID);
        if (peers == null || peers.isEmpty()) {
            System.out.println("The local mesh for node: " + nodeId + " is empty");
            return;
        }
        sendMessageToTopicNodes(m, myPid, topicID, localMesh, avoid, src);
    }

    public void gossipMessageToTopicNodes(Message m, int myPid, String topicID, BigInteger avoid, BigInteger src) {
        m.body = null; // Ensure only metadata is gossiped
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

    private void processCustody(Message m, List<Message> custodyParts, int threshold) {
        if (custodyParts.size() < threshold) {
            long currentTime = CommonState.getTime();
            seedPartArrivalTimeFromPeer.add(currentTime);
            seedPartDelayTimeFromPeer.add(currentTime - m.timestamp);
            seedPartArrivalTimeStore.add(currentTime - m.timestamp);
            custodyParts.add(m);

            if (custodyParts.size() == threshold) {
                createWholeRowOrColumnSeed(m, m.rowOrColumnNumber);
            }
        }
    }

    public void handleReceivedRowOrCol(Message m, int myPid) {
        String s = m.isRow ? "row" : "column";
        String key = s + m.rowOrColumnNumber;

        if (custody1.equals(key)) {
            custodyData1.add(m);
        } else if (custody2.equals(key)) {
            custodyData2.add(m);
        } else {
            return;
        }

        long currentTime = CommonState.getTime();
        messageArrivalTimeFromBP.add(currentTime);
        messageDelayTimeFromBP.add(currentTime - m.timestamp);
        seedArrivalTimeStore.add(currentTime);
    }

    public void samplingStarter() {
        if (samplingStarted)
            return;

        boolean shouldStart = (distributionStrategy == 3 && custodyData1.size() == 1 && custodyData2.size() == 1) ||
                (distributionStrategy == 2 && custodyData1.size() == 2);

        if (shouldStart) {
            samplingStarted = true;
            startSampling();
        }
    }

    public void startSampling() {
        IntStream.range(0, 256).forEach(i -> sampleDataRequest());
    }

    public void handleIHave(Message m, int myPid) {
        if (messageCache.containsKey(m.id)) {
            if (IWANTmessageCache.containsKey(m.id) && m.body != null) { // Late-arriving message
                processReceivedMessage(m, myPid);
            }
            return;
        }

        if (m.ackId == -6) {
            handleAckMessage(m, myPid);
        }

        messageCache.put(m.id, m);

        Message responseWithMetaData = createMessage(m.id, Message.MSG_IHAVE, m.src, m.dest, m.messageTopicID, null,
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), -6);

        Message responseWithData = createMessage(m.id, Message.MSG_IHAVE, m.src, m.dest, m.messageTopicID, m.body,
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), -6);

        responseWithData.src = m.src;

        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, this.nodeId, m.src);
        gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID, this.nodeId, m.src);
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

    // Handle messages with ackId = -6 based on the distribution strategy
    private void handleAckMessage(Message m, int myPid) {
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
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), m.ackId);

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
                m.isRow, m.rowOrColumnNumber, m.partNumber, CommonState.getTime(), m.ackId);

        IWANTmessageCache.put(m.id, m);
        publishMessage(request, m.src, myPid);
    }

    public void handleIWANT(Message m, int myPid) {
        if (distributionStrategy == 3) {
            if (findAndSendResponse(m, myPid, custodyData1) ||
                    findAndSendResponse(m, myPid, custodyData2) ||
                    findAndSendResponse(m, myPid, dataReceivedFromBP)) {
                return;
            }

            System.out.println("I don't have " + m.rowOrColumnNumber +
                    ". I have " + custody2 + " " + custody1 +
                    ". Responding node: " + nodeId + " to " + m.src);
        } else if (distributionStrategy == 2) {
            if (findAndSendResponse(m, myPid, custody1Parts) ||
                    findAndSendResponse(m, myPid, custody2Parts) ||
                    findAndSendResponse(m, myPid, messageCache.values())) {
                return;
            }
        }
    }

    // Helper method to search for the requested message and send a response
    private boolean findAndSendResponse(Message m, int myPid, Collection<Message> messageCollection) {
        for (Message msg : messageCollection) {
            if (m.id == msg.id && msg.isRow == m.isRow &&
                    msg.rowOrColumnNumber == m.rowOrColumnNumber &&
                    m.partNumber == msg.partNumber) {

                Message response = createMessage(m.id, Message.MSG_DATA, nodeId, m.src,
                        m.messageTopicID, (byte[][]) msg.body,
                        m.isRow, m.rowOrColumnNumber, m.partNumber,
                        m.timestamp, m.ackId);

                publishMessage(response, m.src, myPid);
                return true;
            }
        }
        return false;
    }

    public void handleBlockProducerData(Message m, int myPid) {
        GossipSubProtocol proposerNode = (GossipSubProtocol) CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId);

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

        if (m.body == null) {
            System.out.println("Block proposer received empty data!");
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

    // Helper method to send IHAVE responses
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

    public void handleData(Message m, int myPid) {
        if (messageCache.containsKey(m.id) && IWANTmessageCache.containsKey(m.id)) {

            IWANTmessageCache.remove(m.id);
            if (distributionStrategy == 3) {
                handleReceivedRowOrCol(m, myPid);
            } else if (distributionStrategy == 2) {
                handleReceivedPart(m, myPid);
            }
            samplingStarter();
        }
        messageCache.put(m.id, m);
    }

    private Message createMessage(long id, int type, BigInteger src, BigInteger dest, String topicID, Object body,
            boolean isRow, int RowOrColNum, int partNum, long timeStamp, long ackid) {
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

    public void sampleDataRequest() {

        Random random = new Random();

        int rowOrColDecider = random.nextInt(2);
        int randomSampleIndex = random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512));

        int SamplingIdx = randomSampleIndex;
        int rowOrColNo = randomSampleIndex;
        int noOfRowsAndColsInTopic = (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16));
        int topicNo = 0;
        if (noOfRowsAndColsInTopic == 1) {
            if (rowOrColDecider == 0) {
                topicNo = (rowOrColNo * 2) + 1;
            } else {
                topicNo = (rowOrColNo * 2) + 2;
            }
        } else {
            topicNo = rowOrColNo / (noOfRowsAndColsInTopic / 2);
        }
        Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo + 1));
        if (topicToSubscribe == null) {
            // System.out.println("nulll");
            return;
        }
        this.subscribeTopic(topicToSubscribe);

        BigInteger destId = null;
        if (distributionStrategy == 3 || distributionStrategy == 2) {
            if (rowOrColDecider == 0) {
                ArrayList<BigInteger> rowHolders = rowCustodyNodes.get(rowOrColNo);
                int sampleHolderNodeIdx = random.nextInt((rowHolders.size()));
                destId = rowHolders.get(sampleHolderNodeIdx);
            } else {
                ArrayList<BigInteger> colHolders = columnCustodyNodes.get(rowOrColNo);
                int sampleHolderNodeIdx = random.nextInt((colHolders.size()));
                destId = colHolders.get(sampleHolderNodeIdx);
            }
        }
        if (destId == this.nodeId) {
            sampleDataRequest();
            return;
        }

        Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, this.nodeId, destId,
                topicToSubscribe.topicID, Integer.toString(SamplingIdx), rowOrColDecider == 0, rowOrColNo, -1, 0, -1);

        this.sentMsg.put(sampleReqMes.id, sampleReqMes);
        noOfSampleRequestsSent++;

        this.publishMessage(sampleReqMes, destId, gossipSubId);

        peersim.GossipSub.Timeout t = new peersim.GossipSub.Timeout(1, destId, sampleReqMes.id);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(sampleReqMes.dest);
        long latency = transport.getLatency(src, dest);

        EDSimulator.add(4 * latency, t, src, gossipSubId);
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
                            m.partNumber, m.timestamp, m.ackId);
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
                            m.partNumber, m.timestamp, m.ackId);
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

    @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;
        Message m;

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
                    break;
                }
                handleData(m, myPid);
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
                peersim.GossipSub.Timeout t = (Timeout) event;
                if (sentMsg.containsKey(t.msgID)) {
                    this.noOfSampleRequestsSent++;

                    sampleRequestUnsuccessful++;

                    Message sampleMsgSent = sentMsg.get(t.msgID);
                    sentMsg.remove(t.msgID);
                    BigInteger destId;
                    Random random = new Random();
                    if (sampleMsgSent.isRow == true) {
                        ArrayList<BigInteger> rowHolders = rowCustodyNodes.get(sampleMsgSent.rowOrColumnNumber);
                        int sampleHolderNodeIdx = random.nextInt((rowHolders.size()));

                        destId = rowHolders.get(sampleHolderNodeIdx);
                    } else {
                        ArrayList<BigInteger> colHolders = columnCustodyNodes.get(sampleMsgSent.rowOrColumnNumber);
                        int sampleHolderNodeIdx = random.nextInt((colHolders.size()));

                        destId = colHolders.get(sampleHolderNodeIdx);

                    }
                    Message msgToResend = this.createMessage(-1, sampleMsgSent.type, this.nodeId, destId,
                            sampleMsgSent.messageTopicID, sampleMsgSent.body, sampleMsgSent.isRow,
                            sampleMsgSent.rowOrColumnNumber, sampleMsgSent.partNumber, 0, sampleMsgSent.ackId);
                    sentMsg.put(msgToResend.id, msgToResend);

                    publishMessage(msgToResend, msgToResend.dest, myPid);
                    peersim.GossipSub.Timeout timeout = new peersim.GossipSub.Timeout(1, destId, msgToResend.id);
                    Node src = CustomDistribution.networkNodes.get(this.nodeId);
                    Node dest = CustomDistribution.networkNodes.get(destId);
                    long latency = transport.getLatency(src, dest);

                    EDSimulator.add(4 * latency, timeout, src, gossipSubId);
                }
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

    private Message createRowOrColMessageForDistribution(int numberOfCopiesSent, Message newMessage, Block b,
            int rowNumber, String topicId, BigInteger destID, boolean isRow) {
        if (numberOfCopiesSent == 0) {
            byte[][] dataToSend = isRow
                    ? Arrays.copyOfRange(b.getRowData(rowNumber), 0, Configuration.getInt("NUMBER_OF_ROWS") / 2)
                    : Arrays.copyOfRange(b.getColumnData(rowNumber), 0, Configuration.getInt("NUMBER_OF_ROWS") / 2);

            newMessage = new Message(3, dataToSend, true, rowNumber, -1, -1);
            newMessage.src = this.getNodeId();
            newMessage.messageTopicID = topicId;
            newMessage.dest = destID;
        }

        Message messageToSend = createMessage(newMessage.id, 3, this.nodeId, destID, topicId, newMessage.body,
                isRow, rowNumber, -1, -1, -1);

        System.out.println("I am holding " + (isRow ? "row" : "column") + " " + rowNumber + " for " + destID);

        publishMessage(messageToSend, destID, gossipSubId);
        return newMessage;
    }

    private void nCopiesDistributionStrategy() {
        int numberOfCopiesToSend = Configuration.getInt("NUMBER_COPIES_DISTRIBUTED");
        int rowNumber = 0;
        int columnNumber = 0;
        Block b = block;
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));

        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());
        boolean isRowTopic = true;
        boolean isColTopic = false;
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            int cnt = 0;
            int nodeCounter = 0;
            int numberOfCopiesSent = 0;
            System.out.println();
            System.out.println();
            System.out.println("****");
            System.out.println(topicEntry.getKey());
            Message newMessage = null;
            Topic currentTopic = topicEntry.getValue();
            for (Node n : topicEntry.getValue().topicMembers) {
                GossipSubProtocol g = ((GossipSubProtocol) n.getProtocol(gossipSubId));
                if (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC") != 1) {
                    if (cnt < (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC") / 2)) {
                        isRowTopic = true;
                    } else {
                        isRowTopic = false;
                    }
                }
                {
                    if (cnt >= (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC"))) {
                        break;
                    }
                    if (numberOfCopiesSent >= numberOfCopiesToSend
                            && nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC != 0) {
                        nodeCounter++;
                        if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                            numberOfCopiesSent = 0;
                            if (isRowTopic) {
                                rowNumber++;
                            } else {
                                columnNumber++;
                            }
                            cnt++;
                        }
                        continue;
                    }
                    if (isRowTopic && rowNumber == 512) {
                        continue;
                    }
                    if (!isRowTopic && columnNumber == 512) {
                        return;
                    }
                    BigInteger destID = g.getNodeId();
                    Message m1 = createRowOrColMessageForDistribution(numberOfCopiesSent, newMessage, b,
                            isRowTopic == true ? rowNumber : columnNumber, currentTopic.topicID, destID, isRowTopic);
                    if (numberOfCopiesSent == 0) {
                        newMessage = m1;
                    }
                    // System.out.println("I am holding row " + rowNumber + " " + destID);
                    nodeCounter++;
                    numberOfCopiesSent++;
                    if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                        numberOfCopiesSent = 0;
                        if (isRowTopic) {
                            rowNumber++;
                        } else {
                            columnNumber++;
                        }
                        cnt++;
                    }
                    if (!isRowTopic && columnNumber == 512) {
                        return;
                    }
                }
            }
            isRowTopic = isRowTopic == true ? false : true;
            isColTopic = isColTopic == true ? false : true;
        }
        System.out.println(rowNumber);
        System.out.println(columnNumber);
        System.out.println("*********Block proposer has sent the messages ********");
        System.out.println("Data sent size " + totalDataTransmitted);
        System.out.println("Data transmission time " + totalTransmissionTime);
    }

    private void shardingBasedDistribution() {
        int numberOfDivisions = NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC;

        int numberOfCellsInRowOrCol = Configuration.getInt("NUMBER_OF_ROWS");

        int partSize = numberOfCellsInRowOrCol / numberOfDivisions;

        Block b = block;

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));
        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());
        System.out.println("Data sent size " + totalDataTransmitted);

        int rowPartNo;
        int colPartNo;
        int rowNumber = 0;
        int columnNumber = 0;
        boolean isRowTopic = true;
        boolean isColTopic = false;
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            int cnt = 0;
            int nodeCounter = 0;
            int numberOfCopiesSent = 0;
            System.out.println();
            System.out.println();
            System.out.println("****");
            System.out.println(topicEntry.getKey());

            Topic currentTopic = topicEntry.getValue();
            for (Node n : topicEntry.getValue().topicMembers) {
                GossipSubProtocol g = ((GossipSubProtocol) n.getProtocol(gossipSubId));
                if (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC") != 1) {
                    if (cnt < (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC") / 2)) {
                        isRowTopic = true;
                    } else {
                        isRowTopic = false;
                    }
                }
                {
                    if (cnt >= (Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC"))) {
                        break;
                    }
                    if (isRowTopic && rowNumber == 512) {
                        continue;
                    }
                    if (!isRowTopic && columnNumber == 512) {
                        return;
                    }

                    BigInteger destID = g.getNodeId();
                    byte[][] partToSend;
                    if (isRowTopic) {
                        partToSend = Arrays.copyOfRange(b.getRowData(rowNumber), (numberOfCopiesSent) * partSize,
                                ((numberOfCopiesSent) * partSize + partSize));
                    } else {
                        partToSend = Arrays.copyOfRange(b.getColumnData(columnNumber), (numberOfCopiesSent) * partSize,
                                ((numberOfCopiesSent) * partSize + partSize));

                    }
                    Message messageTosend = this.createMessage(-1, 3, this.nodeId, destID, currentTopic.topicID,
                            partToSend, isRowTopic, isRowTopic == true ? rowNumber : columnNumber, numberOfCopiesSent,
                            -1, -1);
                    // System.out.println("I am holding row " + rowNumber + " " +
                    // (numberOfCopiesSent) + " " + destID);
                    this.publishMessage(messageTosend, destID, gossipSubId);

                    nodeCounter++;
                    numberOfCopiesSent++;
                    if (nodeCounter % NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC == 0) {
                        numberOfCopiesSent = 0;
                        if (isRowTopic) {
                            rowNumber++;
                        } else {
                            columnNumber++;
                        }
                        cnt++;
                    }
                    if (!isRowTopic && columnNumber == 512) {
                        return;
                    }
                }
            }
            isRowTopic = isRowTopic == true ? false : true;
            isColTopic = isColTopic == true ? false : true;
        }
        System.out.println("*********Block proposer has sent the messages ********");
        System.out.println("Data sent size " + totalDataTransmitted);
        System.out.println("Data transmission time " + totalTransmissionTime);
    }
}