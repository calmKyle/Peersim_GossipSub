package peersim.GossipSub;

import peersim.GossipSub.Timeout;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;
import peersim.util.IncrementalStats;

// import static peersim.GossipSub.CustomDistribution.networkNodes;
// import static peersim.GossipSub.CustomDistribution.topics;
import static peersim.GossipSub.CustomDistribution.*;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import peersim.core.Protocol;

public class GossipSubProtocol implements Cloneable, EDProtocol {
    // ****VARIABLES REQUIRED FOR GOSSIPSUB****

    // This arraylist contains the rows/cols it was intially given by the block
    // proposer to custody
    // Logic 1: Right now the node will forward the initial data to all the peers in
    // the that topic meaning that eventually all the peers in that topic will hold
    // that data as well.
    // Logic 2: To avoid to forward the initial data received from the data proposer
    // to the nodes in the topic we can data that data to the custodyData arraylist
    // and not to the message cashe and hence not forward that message to our peers.
    // Just send the message with the metadata.

    private long lastMessageTransmissionTime;
    private LinkedHashMap<Long, Message> IWANTmessageCache = new LinkedHashMap<>(); // The transmission time of the
                                                                                    // new/latest message added to the
    // queue(transmission time for the last element in the messageQueue)
    int randomwSampleCounter; // This variable will store a random number when initially initialized and
                              // decremented everytime this node gets the IHAVE message. When it becomes 0 it
                              // will send IWANT message to sending node. It will only be useful in
                              // distributionStragerty 1 because 2 it eventually recieves 8 row/cols
                              // as the validator node has to sample 2 complete rows/cols and 75 samples

    // Constants
    public static final int MESSAGE_CACHE_SIZE = 1024; // Maximum size of messageCache at 1GB
    private static final String PAR_TRANSPORT = "transport";

    // Network parameters
    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;
    public int minDegree = Configuration.getInt("MIN_DEGREE", 4);
    public int maxDegree = Configuration.getInt("MAX_DEGREE", 8);;
    protected int degree = 8;
    private int interfaceBandwidth;
    private int blockProducerBandwidth;
    private int distributionStrategy;

    private int SampleTime = Configuration.getInt("SAMPLETIME", 75);

    protected long totalDataTransmitted;
    protected long totalTransmissionTime;

    // Node information
    private static String prefix;
    public BigInteger nodeId;
    private Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> localMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> topicNodes = new HashMap<>();
    protected Map<String, Set<BigInteger>> gossipMesh = new HashMap<>();

    // Message cache and queues
    private LinkedHashMap<Long, Message> messageCache = new LinkedHashMap<>();
    private ArrayList<Message> messageQueue = new ArrayList<>();
    private ArrayList<Long> messageTransmissionDelayQueue = new ArrayList<>();

    // Data availability sampling statistics
    public IncrementalStats seedArrivalTimeStore = new IncrementalStats();
    public IncrementalStats seedPartArrivalTimeStore = new IncrementalStats();
    public IncrementalStats samplingRTTTimeStore = new IncrementalStats();

    // Message arrival and delay tracking
    public ArrayList<Long> messageArrivalTimeFromBP = new ArrayList<>();
    public ArrayList<Long> messageDelayTimeFromBP = new ArrayList<>();
    public ArrayList<Long> seedPartArrivalTimeFromPeer = new ArrayList<>();
    public ArrayList<Long> seedPartDelayTimeFromPeer = new ArrayList<>();
    public ArrayList<Long> sampleArrivalTime = new ArrayList<>();
    public ArrayList<Long> sampleDelayTime = new ArrayList<>();

    // Request and response counters
    public int sampleRequestUnsuccessful = 0;
    public int NoOfSampleRequestsSent = 0;
    public int NoOfSamplesRecieved = 0;
    public int NoOfSeedPartsRecieved = 0;
    int partRequestCounter = 3;

    // Data custody
    private ArrayList<Message> custodyData1 = new ArrayList<>(); // If distributionStrategy = 1
    private ArrayList<byte[][][]> custodyData2 = new ArrayList<>(); // If distributionStrategy = 2

    // Message tracking
    private TreeMap<Long, Message> sentMsg = new TreeMap<>();
    private TreeMap<Long, Message> sentSeedingPartMsg = new TreeMap<>();

    // Sampling control
    int randomSampleCounter;

    protected String custody1; // This will string will tell which row/col this node will custody
    protected String custody2;

    protected boolean samplingStarted;

    public GossipSubProtocol(String prefix) {
        GossipSubProtocol.prefix = prefix;
        this.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

        // Initialize bandwidth settings
        this.interfaceBandwidth = Configuration.getInt("INTERFACE_BANDWIDTH", 100_000_000);
        this.blockProducerBandwidth = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1_000_000_000);

        // Load distribution strategy
        this.distributionStrategy = Configuration.getInt("DISTRIBUTION_STRATEGY");

        // Randomized sample counter initialization
        Random rnd = new Random();
        this.randomSampleCounter = rnd.nextInt(Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") - 1);
        this.custody1 = null;
        this.custody2 = null;

    }

    public Object clone() {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        return cln;
    }

    // Function to update degree dynamically
    public void setMeshDegree(int newDegree) {
        if (newDegree >= minDegree && newDegree <= maxDegree) {
            this.degree = newDegree;
            updateMeshConnections();
        } else {
            System.out.println("Degree must be between " + minDegree + " and " + maxDegree);
        }
    }

    public void updateMeshConnections() {
        if (subscribedTopics.isEmpty())
            return;

        for (String topicID : subscribedTopics.stream().map(t -> t.topicID).collect(Collectors.toList())) {
            localMesh.putIfAbsent(topicID, new HashSet<>()); // Ensure localMesh is initialized

            Set<BigInteger> peers = localMesh.get(topicID);
            if (peers == null)
                continue;

            // If mesh size is too small, add more peers
            if (peers.size() < degree) {
                addMorePeers(topicID, degree - peers.size());
            }

            // If mesh size is too large, remove excess peers
            if (peers.size() > degree) {
                removeExcessPeers(topicID, peers.size() - degree);
            }
        }
    }

    // Function to add new peers when the mesh is too small
    private void addMorePeers(String topicID, int count) {
        Topic topic = CustomDistribution.topics.get(topicID);
        if (topic == null)
            return;

        List<Node> potentialPeers = new ArrayList<>(topic.topicMembers);
        Collections.shuffle(potentialPeers);

        for (Node newPeer : potentialPeers) {
            if (count <= 0)
                break;

            GossipSubProtocol peerNode = (GossipSubProtocol) newPeer.getProtocol(gossipSubId);
            if (!peerNode.localMesh.get(topicID).contains(this.nodeId)) {
                // Establish bi-directional connection
                peerNode.localMesh.get(topicID).add(this.nodeId);
                this.localMesh.get(topicID).add(peerNode.nodeId);
                count--;
            }
        }
    }

    // Function to remove excess peers when the mesh is too large
    private void removeExcessPeers(String topicID, int count) {
        if (!localMesh.containsKey(topicID))
            return;

        Iterator<BigInteger> iterator = localMesh.get(topicID).iterator();
        while (iterator.hasNext() && count > 0) {
            BigInteger peerID = iterator.next();
            iterator.remove();
            count--;

            // Remove the reference from the peer's localMesh as well
            GossipSubProtocol peerNode = (GossipSubProtocol) CustomDistribution.networkNodes.get(peerID)
                    .getProtocol(gossipSubId);
            peerNode.localMesh.get(topicID).remove(this.nodeId);
        }
    }

    // Calculating the size of the message in bytes
    private int calculateMessageSize(Message message) {
        int size = Integer.BYTES // Message Type
                + Long.BYTES // Message ID
                + message.src.toString().getBytes().length // Source ID size
                + message.dest.toString().getBytes().length; // Destination ID size

        if (message.messageTopicID != null) {
            size += message.messageTopicID.getBytes().length; // Topic ID size
        }

        if (message.body != null) {
            if (message.body instanceof byte[][] bodyArray) { // 2D array
                for (byte[] row : bodyArray) {
                    size += row.length;
                }
            } else if (message.body instanceof String bodyString) {
                size += bodyString.getBytes().length; // String size
            } else if (message.body instanceof byte[][][] bodyArray3D) { // 3D array
                for (byte[][] matrix : bodyArray3D) {
                    for (byte[] row : matrix) {
                        size += row.length;
                    }
                }
            }
        }

        size += 1 // isRow flag
                + Integer.BYTES // rowOrColumnNumber
                + Integer.BYTES // partNumber
                + Long.BYTES; // timestamp

        return size;
    }

    // Topic subscription function. Used to subscribe to a particular topic
    public void subscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            return;
        }
        subscribedTopics.add(topic);
    }

    // Topic unsubscription function
    public void unsubscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            subscribedTopics.remove(topic);
            localMesh.remove(topic.topicID);
            // topic.removeMember(networkNodes.get(this.nodeId));
            return;
        }
    }

    // Function to add a topic to which this node is subsribed and its member
    public void setTopicMembersList(String topicName, Set<BigInteger> members) {
        topicNodes.put(topicName, members);
    }

    // Function to send the message in the network
    public void publishMessage(Message m, BigInteger destId, int myPid) {
        // Determine bandwidth
        int bandwidth = (this.nodeId == ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId))).nodeId) ? blockProducerBandwidth : interfaceBandwidth;

        // Cache the message
        messageCache.put(m.id, m);

        // Get source and destination nodes
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        // Compute network latency
        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        final long latency = transport.getLatency(src, dest);

        // Process message queue
        processMessageQueue();

        // Compute delays
        final int messageSize = calculateMessageSize(m);
        final long transmissionDelay = (long) Math.ceil((double) messageSize / bandwidth);
        final long propagationDelay = latency;
        final long totalDelay = calculateTotalDelay(transmissionDelay, propagationDelay);

        // Update transmission statistics
        totalDataTransmitted += messageSize;
        totalTransmissionTime += totalDelay;

        // Schedule message transmission
        EDSimulator.add(totalDelay, m, dest, myPid);

        // Update message queue tracking
        final long waitUntilMessageSent = CommonState.getTime() + transmissionDelay;
        messageQueue.add(m);
        messageTransmissionDelayQueue.add(waitUntilMessageSent);
        lastMessageTransmissionTime = waitUntilMessageSent;
    }

    /**
     * Removes messages from the queue if they have already been sent.
     */
    private void processMessageQueue() {
        while (!messageTransmissionDelayQueue.isEmpty() &&
                CommonState.getTime() > messageTransmissionDelayQueue.get(0)) {
            messageQueue.remove(0);
            messageTransmissionDelayQueue.remove(0);
        }
    }

    /**
     * Calculates the total delay for message transmission.
     */
    private long calculateTotalDelay(long transmissionDelay, long propagationDelay) {
        if (messageQueue.isEmpty()) {
            return transmissionDelay + propagationDelay;
        }
        long queuingDelay = messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1)
                - CommonState.getTime();
        return queuingDelay + transmissionDelay + propagationDelay;
    }

    // Send Message to all nodes in topic
    public void sendMessageToTopicNodes(Message m, int myPid, String topicID,
            Map<String, Set<BigInteger>> nodesInTopic,
            BigInteger messageSender) {
        Set<BigInteger> topicNodes = nodesInTopic.get(topicID);

        if (topicNodes == null || topicNodes.isEmpty()) {
            return; // No nodes to send messages to
        }

        for (BigInteger peerId : topicNodes) {
            if (peerId.equals(messageSender) || peerId.equals(m.src)) {
                continue; // Skip sender and source node
            }

            Message newMessage = createMessageForPeer(m, peerId);
            publishMessage(newMessage, peerId, myPid);
        }
    }

    /**
     * Creates a message for a given peer.
     */
    private Message createMessageForPeer(Message m, BigInteger peerId) {
        boolean isGossipMessage = (m.type == Message.MSG_IHAVE && m.body == null
                && m.src != ((GossipSubProtocol) CustomDistribution.blockProposerNode
                        .getProtocol(gossipSubId)).nodeId);

        return this.createMessage(
                m.id,
                m.type,
                isGossipMessage ? m.src : peerId,
                isGossipMessage ? peerId : this.nodeId,
                m.messageTopicID,
                m.body,
                m.isRow,
                m.rowOrColumnNumber,
                m.partNumber,
                m.timestamp,
                m.ackId == -6 ? m.ackId : m.id);
    }

    // Send message to all the peers/local mesh in the given topic
    public void sendMessageToPeers(Message m, int myPid, String topicID, BigInteger avoid) {
        if (this.localMesh.get(topicID).isEmpty()) {
            System.out.println("The local mesh for node: " + this.nodeId + " is empty");
            return;
        }
        sendMessageToTopicNodes(m, myPid, topicID, this.localMesh, avoid);
    }

    // Send the Metadata all the nodes/global mesh in the topic
    // Might implement that a random no of nodes are selected for
    // gossiping-------------------
    public void gossipMessageToTopicNodes(Message m, int myPid, String topicID, BigInteger avd) {
        m.body = null;
        sendMessageToTopicNodes(m, myPid, topicID, this.topicNodes, avd);
    }

    // Function to handle the recieved IHave messages

    public void handleIHave(Message m, int myPid) {
        // If message already exists in cache
        if (messageCache.containsKey(m.id)) {
            handleLateMessage(m);
            return;
        }

        // If using distribution strategy 3 and ackId is -6, process custody-related
        // logic
        if (distributionStrategy == 3 && m.ackId == -6) {
            if (custodyData1.contains(m)) {
                System.out.println("Message already received by node " + this.nodeId);
                return;
            }

            handleCustodyMessage(m, myPid);
        }
    }

    /**
     * Handles the case where a message arrives late after the node has sent an
     * IHAVE message.
     */
    private void handleLateMessage(Message m) {
        if (IWANTmessageCache.containsKey(m.id) && m.body != null) {
            long arrivalTime = CommonState.getTime();
            long delayTime = arrivalTime - m.timestamp;

            messageArrivalTimeFromBP.add(arrivalTime);
            messageDelayTimeFromBP.add(delayTime);
            seedArrivalTimeStore.add(delayTime);

            custodyData1.add(m);
            messageCache.put(m.id, m);
            IWANTmessageCache.remove(m.id);

            if (custodyData1.size() == 2 && !samplingStarted) {
                samplingStarted = true;
                startSampling();
            }
        }
    }

    /**
     * Handles custody-related logic when a message is received.
     */
    private void handleCustodyMessage(Message m, int myPid) {
        boolean isRowMatch = custody1.equals("row" + m.rowOrColumnNumber)
                || custody2.equals("row" + m.rowOrColumnNumber);
        boolean isColumnMatch = custody1.equals("column" + m.rowOrColumnNumber)
                || custody2.equals("column" + m.rowOrColumnNumber);

        if (m.isRow && isRowMatch || !m.isRow && isColumnMatch) {
            if (m.body == null) {
                sendIWantMessage(m, myPid);
            } else {
                recordMessageArrival(m);
            }
        }

        messageCache.put(m.id, m);
        forwardGossipMessage(m, myPid);
    }

    /**
     * Sends an IWANT message requesting the missing message body.
     */
    private void sendIWantMessage(Message m, int myPid) {
        Message request = createMessage(m.id, Message.MSG_IWANT, this.nodeId, m.src, m.messageTopicID, "",
                m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.ackId);

        IWANTmessageCache.put(m.id, m);
        publishMessage(request, m.src, myPid);
    }

    /**
     * Forwards the received message as a gossip message to peers.
     */
    private void forwardGossipMessage(Message m, int myPid) {
        Message responseWithMetaData = createMessage(m.id, Message.MSG_IHAVE, m.src, m.dest, m.messageTopicID,
                null, m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, -6);

        Message responseWithData = createMessage(m.id, Message.MSG_IHAVE, m.src, m.dest, m.messageTopicID,
                m.body, m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, -6);

        responseWithData.src = m.src;

        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, this.nodeId);
        gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID, this.nodeId);
    }

    // Send data to the node who requested/sent IWant message
    // Compare the Iwant messages with the requested data message and send that
    public void handleIWANT(Message m, int myPid) {
        if (custodyData1.isEmpty()) {
            // No custody data available, so no response needed
            return;
        }

        for (Message msg : custodyData1) {
            if (m.id == msg.id && m.isRow == msg.isRow && m.rowOrColumnNumber == msg.rowOrColumnNumber
                    && m.partNumber == msg.partNumber) {

                // Ensure the body is not null before attempting to cast
                if (msg.body instanceof byte[][] responseData) {
                    Message response = createMessage(m.id, Message.MSG_DATA, this.nodeId, m.src,
                            m.messageTopicID, responseData, m.isRow,
                            m.rowOrColumnNumber, m.partNumber,
                            m.timestamp, m.ackId);

                    publishMessage(response, m.src, myPid);
                }
                return; // Exit after handling the first match
            }
        }
    }

    public void handleBlockProducerData(Message m, int myPid) {
        GossipSubProtocol proposerProtocol = (GossipSubProtocol) CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId);

        // Process only if message is from the block proposer
        if (m.src != proposerProtocol.nodeId) {
            return;
        }

        if (shouldProcessMessage(m)) {
            recordMessageArrival(m);

            // Maintain message cache size
            if (messageCache.size() > MESSAGE_CACHE_SIZE) {
                messageCache.remove(messageCache.keySet().iterator().next());
            }

            messageCache.put(m.id, m);

            // Handle custody data if applicable
            if (m.partNumber == -1 && m.rowOrColumnNumber != -1 && distributionStrategy == 3) {
                handleCustodyData(m, myPid);
            }
        }
    }

    /**
     * Determines if the message should be processed.
     */
    private boolean shouldProcessMessage(Message m) {
        return !messageCache.containsKey(m.id) || IWANTmessageCache.containsKey(m.id);
    }

    /**
     * Records message arrival and delay times.
     */
    private void recordMessageArrival(Message m) {
        long currentTime = CommonState.getTime();
        long delayTime = currentTime - m.timestamp;

        messageArrivalTimeFromBP.add(currentTime);
        messageDelayTimeFromBP.add(delayTime);
        seedArrivalTimeStore.add(delayTime);
    }

    /**
     * Handles custody data storage and response messages.
     */
    private void handleCustodyData(Message m, int myPid) {
        custodyData1.add(m);

        if (IWANTmessageCache.containsKey(m.id)) {
            IWANTmessageCache.remove(m.id);
        }

        createAndSendResponseMessages(m, myPid);

        if (custodyData1.size() == 2 && !samplingStarted) {
            samplingStarted = true;
            startSampling();
        }
    }

    /**
     * Creates and sends IHAVE response messages.
     */
    private void createAndSendResponseMessages(Message m, int myPid) {
        Message responseWithMetaData = createMessage(m.id, Message.MSG_IHAVE, this.nodeId, m.dest,
                m.messageTopicID, null, m.isRow,
                m.rowOrColumnNumber, m.partNumber,
                m.timestamp, -6);

        Message responseWithData = createMessage(m.id, Message.MSG_IHAVE, this.nodeId, m.dest,
                m.messageTopicID, m.body, m.isRow,
                m.rowOrColumnNumber, m.partNumber,
                m.timestamp, -6);

        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, this.nodeId);
        gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID, this.nodeId);
    }

    // This function handles the data received
    public void handleData(Message m, int myPid) {
        if (!messageCache.containsKey(m.id) || !IWANTmessageCache.containsKey(m.id)) {
            // If the message is not in cache or not requested, ignore it
            messageCache.put(m.id, m);
            return;
        }

        // Process message arrival and delay
        recordMessageArrival(m);

        // Remove from IWANT cache since the data has arrived
        IWANTmessageCache.remove(m.id);

        // Process custody data
        processCustodyData(m);
    }

    /**
     * Adds the message to custody and starts sampling if conditions are met.
     */
    private void processCustodyData(Message m) {
        custodyData1.add(m);

        if (custodyData1.size() == 2 && !samplingStarted) {
            samplingStarted = true;
            startSampling();
        }
    }

    // Function the create the Message instance
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

    // Randomly find the row or column you want to sample
    // random the index for that row or col
    // Subscribe to the topic to which the row or col belongs to
    // Find the node ID of the node who is holding the required sample data
    // Send the message
    // In the message data send an idx of the row/col to be sampled

    /** This sample Data Request will be replace by the PANDAS paper later on */
    public void sampleDataRequest() {
        Random random = new Random();

        // Determine whether to sample from row (0) or column (1)
        final int rowOrColDecider = random.nextInt(2);
        final int randomSampleIndex = random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512));

        // Calculate topic number
        final int numRowsColsInTopic = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC", 16);
        final int topicNo = randomSampleIndex / (numRowsColsInTopic / 2);
        final Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo + 1));

        if (topicToSubscribe == null) {
            return; // Exit if topic doesn't exist
        }

        this.subscribeTopic(topicToSubscribe);

        // Determine target node for sampling
        BigInteger destId = getDestinationNode(rowOrColDecider, randomSampleIndex, random);

        if (destId == null || destId.equals(this.nodeId)) {
            return; // Avoid sending request to itself
        }

        // Create and send the sample request message
        Message sampleReqMes = createMessage(
                -1,
                Message.MSG_SAMPLE_DATA_REQUEST,
                this.nodeId,
                destId,
                topicToSubscribe.topicID,
                Integer.toString(randomSampleIndex),
                rowOrColDecider == 0,
                randomSampleIndex,
                -1,
                0,
                -1);

        this.sentMsg.put(sampleReqMes.id, sampleReqMes);
        NoOfSampleRequestsSent++;

        this.publishMessage(sampleReqMes, destId, gossipSubId);

        // Schedule timeout for request
        peersim.GossipSub.Timeout timeoutEvent = new peersim.GossipSub.Timeout(1, destId, sampleReqMes.id);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(sampleReqMes.dest);
        long latency = transport.getLatency(src, dest);

        EDSimulator.add(4 * latency, timeoutEvent, src, gossipSubId);
    }

    /**
     * Determines the destination node for sampling based on distribution strategy.
     */
    private BigInteger getDestinationNode(int rowOrColDecider, int rowOrColNo, Random random) {
        if (distributionStrategy != 3) {
            return null;
        }

        ArrayList<BigInteger> nodeHolders = (rowOrColDecider == 0)
                ? rowCustodyNodes.get(rowOrColNo)
                : columnCustodyNodes.get(rowOrColNo);

        if (nodeHolders == null || nodeHolders.isEmpty()) {
            return null;
        }

        return nodeHolders.get(random.nextInt(nodeHolders.size()));
    }

    /**
     * Starts the sampling process. (at least 73)
     */
    public void startSampling() {
        for (int i = 0; i < SampleTime; i++) {
            sampleDataRequest();
        }
    }

    /**
     * Handles a sample request by checking if the requested data is available
     * and sending a response if found.
     *
     * @param m     The message containing the sample request.
     * @param myPid The process ID of the current node.
     */
    public void handleSampleRequest(Message m, int myPid) {
        final boolean requestedRow = m.isRow;
        final int rowOrColNumber = m.rowOrColumnNumber;
        final int idx = Integer.parseInt((String) m.body);

        // Get block proposer ID
        final BigInteger blockProposerId = ((GossipSubProtocol) CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId)).nodeId;

        // Check if custody is empty
        if (custody1.isEmpty()) {
            System.out.println(
                    "Custody is empty. Holding: " + custody1 + " " + custody2 + ". Requested: " + rowOrColNumber);
            return;
        }

        // Find custody message
        Message custodyMessage = findCustodyMessage(m);
        if (custodyMessage != null) {
            Message sampleResponse = createSampleResponse(m, idx, custodyMessage);
            publishMessage(sampleResponse, m.src, gossipSubId);
            return;
        }

        // Check if requested row/column is held
        boolean holdsRequestedData = (requestedRow
                && (custody1.equals("row" + rowOrColNumber) || custody2.equals("row" + rowOrColNumber))) ||
                (!requestedRow
                        && (custody1.equals("column" + rowOrColNumber) || custody2.equals("column" + rowOrColNumber)));

        if (!holdsRequestedData) {
            System.out.println("No custody for requested " + (requestedRow ? "row" : "column") + ": " + rowOrColNumber);
        }
    }

    /**
     * Finds a custody message matching the request.
     */
    private Message findCustodyMessage(Message m) {
        for (Message custodyMessage : custodyData1) {
            if (m.isRow == custodyMessage.isRow && m.rowOrColumnNumber == custodyMessage.rowOrColumnNumber) {
                return custodyMessage;
            }
        }
        return null;
    }

    /**
     * Creates a sample response message.
     */
    private Message createSampleResponse(Message m, int idx, Message custodyMessage) {
        byte[][] data = (byte[][]) custodyMessage.body;
        byte sampleDataResp = data[0][idx];

        return createMessage(
                m.id,
                Message.MSG_SAMPLE_DATA_RESPONSE,
                this.nodeId,
                m.src,
                m.messageTopicID,
                Integer.toString(sampleDataResp),
                m.isRow,
                m.rowOrColumnNumber,
                m.partNumber,
                m.timestamp,
                m.ackId);
    }

    /**
     * Handles the arrival of a sample response by recording its timing details
     * and updating the count of received samples.
     *
     * @param m     The message containing the sample response.
     * @param myPid The process ID of the current node.
     */
    public void handleSampleResponse(Message m, int myPid) {
        // System.out.println("Received the samples");

        final long currentTime = CommonState.getTime();
        final long delayTime = currentTime - m.timestamp;

        sampleArrivalTime.add(currentTime);
        sampleDelayTime.add(delayTime);
        samplingRTTTimeStore.add(currentTime);

        NoOfSamplesRecieved++;
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

    public void healMesh() {
        for (String topicID : subscribedTopics.stream().map(t -> t.topicID).collect(Collectors.toList())) {
            if (!localMesh.containsKey(topicID)) continue;
    
            Set<BigInteger> peers = localMesh.get(topicID);
            if (peers.size() < minDegree) {
                // Add more peers to restore connectivity
                int neededPeers = minDegree - peers.size();
                TopicBasedMesh meshManager = new TopicBasedMesh(GossipSubProtocol.prefix);
                meshManager.addMorePeers(this, topicID, neededPeers);
                System.out.println("Mesh healed for " + topicID + ", added " + neededPeers + " peers.");
            }
        }
    }
    

    // Trying to implement
    // The validator can recontruct the row/columns if it has 50%(256 cells) of each
    // row/col
    // The BP does send the whole row/col divided into parts, where number of parts
    // equals to number of rows/cols in a topic.
    // So the first 8 nodes in the topic will hold the row parts and the next 8
    // nodes will hold the column parts
    // So the first node will fold the 1st parts of all the 8 rows which are divided
    // into 8 parts

    /**
     * The block proposer distributes row and column data to nodes in different
     * topics.
     */
    private void nCopiesDistributionStrategy() {

        int numberOfCopiesToSend = Configuration.getInt("NUMBER_COPIES_DISTRIBUTED", 1);
        int rowNumber = 0;
        int columnNumber = 0;

        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS"));

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId));

        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
            int cnt = 0;
            int nodeCounter = 0;
            int numberOfCopiesSent = 0;
            System.out.println();
            System.out.println();
            System.out.println("****");
            System.out.println(topicEntry.getKey());
            for (Node n : topicEntry.getValue().topicMembers) {
                if (cnt < (Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2)) {
                    GossipSubProtocol g = ((GossipSubProtocol) n.getProtocol(gossipSubId));

                    // System.out.println(g.custody1);
                    // System.out.println(g.custody2);

                    if (numberOfCopiesSent >= numberOfCopiesToSend && nodeCounter % 8 != 0) {
                        nodeCounter++;
                        if (nodeCounter % 8 == 0) {
                            numberOfCopiesSent = 0;
                            rowNumber++;
                            cnt++;
                        }
                        continue;
                    }
                    byte[][] rowToSend = b.getRowData(rowNumber);

                    BigInteger destID = g.getNodeId();

                    Message newMessage = new Message(3, rowToSend, true, rowNumber, -1, -1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;

                    newMessage.dest = destID;
                    System.out.println("I am holding row " + rowNumber + " " + destID);

                    this.publishMessage(newMessage, destID, gossipSubId);

                    nodeCounter++;
                    numberOfCopiesSent++;
                    // return;
                } else {

                    GossipSubProtocol g = ((GossipSubProtocol) n.getProtocol(gossipSubId));

                    // System.out.println(g.custody1);
                    // System.out.println(g.custody2);

                    if (numberOfCopiesSent >= numberOfCopiesToSend && nodeCounter % 8 != 0) {
                        nodeCounter++;
                        if (nodeCounter % 8 == 0) {
                            numberOfCopiesSent = 0;
                            columnNumber++;
                            cnt++;
                            // columnCustodyNodes.put(columnNumber,new ArrayList<>());
                        }
                        continue;
                    }
                    if (columnNumber == 512) {
                        return;
                    }

                    byte[][] colToSend = b.getColumnData(columnNumber);

                    BigInteger destID = g.getNodeId();

                    Message newMessage = new Message(3, colToSend, false, columnNumber, -1, -1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    newMessage.dest = destID;

                    this.publishMessage(newMessage, destID, gossipSubId);
                    nodeCounter++;
                    numberOfCopiesSent++;

                    // return;
                }
            }
            // return;
        }
        System.out.println(rowNumber);
        System.out.println(columnNumber);
        System.out.println("*********Block proposer has sent the messages ********");
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
                } else {
                    // System.out.println("Already removed");
                }
                break;

            case peersim.GossipSub.Timeout.TIMEOUT: // timeout
                peersim.GossipSub.Timeout t = (Timeout) event;
                if (sentMsg.containsKey(t.msgID)) {
                    this.NoOfSampleRequestsSent++;

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
            // TO DO

        }
    }
}

// Trying to implement
// The validator can recontruct the row/columns if it has 50%(256 cells) of each
// row/col
// The BP doesn't send the whole row/col
// The BP sends the half of the whole row/col which is divided into 2 parts

// Compile command; Open terminal in src dir.
// javac -cp "../lib/peersim-1.0.5.jar;../lib/other-dependency.jar" -d
// ../classes peersim\GossipSub\*.java
// Run the stimulator; open terminal in PROJECT directory
// java -cp "classes;lib\peersim-1.0.5.jar;lib\jep-2.3.0.jar;lib\djep-1.0.0.jar"
// peersim.Simulator Config1.cfg

// Add the body to all the messages -----------------------------------------
// Do all the nodes in the mesh send IHave message for the same
// messgae/Date?-------
// How? what is added in the queue?How big is it? QUESTION----------

// For sampling, you can start after you receives the data from the block
// producer
// To determine who holds which row or column, you can a global state; loop over
// each Validator node and find what they hold using the RowColumnDistributor
// To implement bandwidth, keep a queue for indivual gossipsub protocol instace
// storing the message they want to send
// Keep a time variable, waitUntilMessageSent in Long data type, which stores
// the when the sent message will reach, so to wait until the previous message
// is completely sent before sending the next on. Add this waiting time in
// latency

// Note we are assuming
// 1.There are no mallicious nodes
// 2. The transport protocol is reliable(No data is lost)
