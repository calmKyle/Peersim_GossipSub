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
    }

    public Object clone() {
        return new GossipSubProtocol(GossipSubProtocol.prefix);
    }

     public void setNodeId(BigInteger nodeId) {
        this.nodeId = nodeId;
    }

    public BigInteger getNodeId() {
        return nodeId;
    }

    public void setTopicMembersList(String ID, Set<BigInteger> members) {
        String topicKey = "Topic-" + ID;
    
        if (!topics.containsKey(topicKey)) {
            System.err.println("Topic " + topicKey + " does not exist.");
            return;
        }
    
        Topic topic = topics.get(topicKey);
        topic.topicMembers.clear(); // Clear existing members
    
        for (BigInteger nodeId : members) {
            if (networkNodes.containsKey(nodeId)) {
                topic.addMember(networkNodes.get(nodeId)); // Add node to topic
            }
        }
    
        topicNodes.put(topicKey, new HashSet<>(members)); // Update the local mapping
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
        }
    }

    public void unsubscribeTopic(Topic topic) {
        if (subscribedTopics.remove(topic)) {
            localMesh.remove(topic.topicID);
        }
    }

    public void publishMessage(Message m, BigInteger destId, int myPid) {
        int bandwidth = (this.nodeId.equals(
                ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId))
                        ? blockProducerBandwidth
                        : interfaceBandwidth;

        this.messageCache.put(m.id, m);

        long queuingDelay = (!messageQueue.isEmpty())
                ? messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1) - CommonState.getTime()
                : 0;

        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        long latency = transport.getLatency(src, dest);

        int messageSize = calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / bandwidth);
        long totalDelay = queuingDelay + transmissionDelay + latency;

        EDSimulator.add(totalDelay, m, dest, myPid);

        long waitUntilMessageSent = CommonState.getTime() + transmissionDelay;
        messageQueue.add(m);
        messageTransmissionDelayQueue.add(waitUntilMessageSent);
        lastMessageTransmissionTime = waitUntilMessageSent;
    }

    public void handleIHave(Message m, int myPid) {
        if (!messageCache.containsKey(m.id)) {
            if (distributionStrategy == 1 && randomSampleCounter == 0) {
                Message request = createMessage(m.id, Message.MSG_IWANT, m.src, m.messageTopicID, "",
                        m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.ackId);
                publishMessage(request, m.src, myPid);
            }
            randomSampleCounter--;
            messageCache.put(m.id, m);
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
        }
    }

    public void handleData(Message m, int myPid) {
        if (!messageCache.containsKey(m.id)) {
            messageCache.put(m.id, m);
            if (sentSeedingPartMsg.containsKey(m.id)) {
                seedPartArrivalTimeFromPeer.add(CommonState.getTime());
                seedPartDelayTimeFromPeer.add(CommonState.getTime() - m.timestamp);
                seedPartArrivalTimeStore.add(CommonState.getTime() - m.timestamp);
                NoOfSeedPartsRecieved++;
                sentSeedingPartMsg.remove(m.id);
                custodyData2.add((byte[][][]) m.body);
                partRequestCounter--;
            }
        }
    }

    public void handleSampleRequest(Message m, int myPid) {
        boolean requestedRow = m.isRow;
        int rowOrColNumb = m.rowOrColumnNumber;
        int idx = Integer.parseInt((String) m.body);

        for (Map.Entry<Long, Message> msg : messageCache.entrySet()) {
            Message curMessage = msg.getValue();
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

    private Message createMessage(long id, int type, BigInteger dest, String topicID, Object body, boolean isRow,
            int rowOrColNum, int partNum, long timeStamp, long ackid) {
        Message msg = (id == -1) ? new Message(type, isRow, rowOrColNum, partNum, ackid)
                : new Message(id, type, isRow, rowOrColNum, partNum, ackid);

        msg.timestamp = (id == -1) ? CommonState.getTime() : timeStamp;
        msg.src = this.nodeId;
        msg.dest = dest;
        msg.messageTopicID = topicID;
        msg.body = body;
        return msg;
    }

    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;
        Message m;

        switch (((SimpleEvent) event).getType()) {
            case Message.MSG_IHAVE:
                handleIHave((Message) event, myPid);
                break;
            case Message.MSG_IWANT:
                handleIWANT((Message) event, myPid);
                break;
            case Message.MSG_DATA:
                m = (Message) event;
                if (m.src.equals(
                        ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId)) {
                    handleBlockProducerData(m, myPid);
                } else {
                    handleData(m, myPid);
                }
                break;
            case Message.MSG_SAMPLE_DATA_REQUEST:
                handleSampleRequest((Message) event, myPid);
                break;
            case Message.MSG_SAMPLE_DATA_RESPONSE:
                if (sentMsg.containsKey(((Message) event).id)) {
                    sentMsg.remove(((Message) event).id);
                    handleSampleResponse((Message) event, myPid);
                }
                break;
            case Timeout.TIMEOUT:
                handleTimeout((Timeout) event, myPid);
                break;
            case Message.MSG_RESET_BANDWIDTH:
                handleBandwidthReset((Message) event, myPid);
                break;
        }
    }
}
