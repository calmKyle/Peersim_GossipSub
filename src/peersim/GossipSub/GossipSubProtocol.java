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

import static peersim.GossipSub.CustomDistribution.networkNodes;
import static peersim.GossipSub.CustomDistribution.topics;

import java.math.BigInteger;
import java.util.*;
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

    private long lastMessageTransmissionTime; // The transmission time of the new/latest message added to the
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
    private int degreeLow = 6;
    private int degreeHigh = 14;
    protected int degree = 8;
    private int interfaceBandwidth;
    private int blockProducerBandwidth;
    private int distributionStrategy;

    // Node information
    private static String prefix;
    public BigInteger nodeId;
    private Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> localMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> topicNodes = new HashMap<>();

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
    private ArrayList<byte[][]> custodyData1 = new ArrayList<>(); // If distributionStrategy = 1
    private ArrayList<byte[][][]> custodyData2 = new ArrayList<>(); // If distributionStrategy = 2

    // Message tracking
    private TreeMap<Long, Message> sentMsg = new TreeMap<>();
    private TreeMap<Long, Message> sentSeedingPartMsg = new TreeMap<>();

    // Sampling control
    int randomSampleCounter;

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
    }

    public Object clone() {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        return cln;
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
    public void publishMessage(Message message, BigInteger destinationId, int protocolId) {
        int bandwidth = (this.nodeId == getBlockProposerNodeId()) ? blockProducerBandwidth : interfaceBandwidth;
        messageCache.put(message.id, message);

        Node source = CustomDistribution.networkNodes.get(this.nodeId);
        Node destination = CustomDistribution.networkNodes.get(message.dest);
        transport = (UnreliableTransport) Network.prototype.getProtocol(tid);

        long latency = transport.getLatency(source, destination);
        long queuingDelay = calculateQueuingDelay();
        int messageSize = calculateMessageSize(message);
        long transmissionDelay = (long) Math.ceil((double) messageSize / bandwidth);
        long totalDelay = queuingDelay + transmissionDelay + latency;

        EDSimulator.add(totalDelay, message, destination, protocolId);
        updateMessageQueue(message, transmissionDelay);
    }

    private BigInteger getBlockProposerNodeId() {
        GossipSubProtocol proposerProtocol = (GossipSubProtocol) CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId);
        return proposerProtocol.nodeId;
    }

    private long calculateQueuingDelay() {
        while (!messageTransmissionDelayQueue.isEmpty()
                && CommonState.getTime() > messageTransmissionDelayQueue.get(0)) {
            messageQueue.remove(0);
            messageTransmissionDelayQueue.remove(0);
        }
        return messageQueue.isEmpty() ? 0
                : messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1) - CommonState.getTime();
    }

    private void updateMessageQueue(Message message, long transmissionDelay) {
        long messageSentTime = CommonState.getTime() + transmissionDelay;
        messageQueue.add(message);
        messageTransmissionDelayQueue.add(messageSentTime);
        lastMessageTransmissionTime = messageSentTime;
    }

    // Send Message to all nodes in topic
    public void sendMessageToTopicNodes(Message m, int myPid, String topicID,
            Map<String, Set<BigInteger>> nodesInTopic,
            BigInteger messageSender) {
        Set<BigInteger> topicNodes = nodesInTopic.getOrDefault(topicID, Collections.emptySet());

        for (BigInteger peerId : topicNodes) {
            if (peerId.equals(messageSender)) { // Avoid sending back to the sender
                continue;
            }

            Message newMessage = createMessage(-1, m.type, peerId, m.messageTopicID, m.body,
                    m.isRow, m.rowOrColumnNumber, m.partNumber,
                    m.timestamp, m.id);
            publishMessage(newMessage, peerId, myPid);
        }
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
        sendMessageToTopicNodes(m, myPid, topicID, this.topicNodes, avd);
    }

    // Function to handle the recieved IHave messages

    public void handleIHave(Message m, int myPid) {
        // Return if the message is already in the cache
        if (messageCache.containsKey(m.id)) {
            return;
        }

        // Introduce randomness in deciding whether to request the data
        Random random = new Random();
        boolean shouldRequest = random.nextDouble() < 0.5; // 50% probability of requesting the data

        if (distributionStrategy == 1 && randomSampleCounter == 0 && shouldRequest) {
            // Check if we already hold the same row/column to avoid redundant requests
            for (Message cachedMsg : messageCache.values()) {
                if (cachedMsg.isRow == m.isRow && cachedMsg.rowOrColumnNumber == m.rowOrColumnNumber) {
                    return;
                }
            }

            // Create and publish an IWANT request
            Message request = createMessage(m.id, Message.MSG_IWANT, m.src, m.messageTopicID, "",
                    m.isRow, m.rowOrColumnNumber, m.partNumber,
                    m.timestamp, m.ackId);
            publishMessage(request, m.src, myPid);
            System.out.println("Node " + myPid + " requested data from Node " + m.src);
        }

        // Decrement the counter and cache the message
        randomSampleCounter--;
        messageCache.put(m.id, m);
    }

    // Send data to the node who requested/sent IWant message
    // Compare the Iwant messages with the requested data message and send that
    public void handleIWANT(Message m, int myPid) {
        // Check if the message is in cache and respond if using strategy 1
        if (distributionStrategy == 1 && messageCache.containsKey(m.ackId)) {
            Message response = createMessage(m.id, Message.MSG_DATA, m.src,
                    m.messageTopicID, "",
                    m.isRow, m.rowOrColumnNumber, m.partNumber,
                    m.timestamp, m.ackId);
            publishMessage(response, m.src, myPid);
            System.out.println("Node " + myPid + " sent DATA response to Node " + m.src);
            return;
        }

        // If custodyData2 is not empty, check if the requested data exists in cache
        if (!custodyData2.isEmpty()) {
            for (Message cachedMsg : messageCache.values()) {
                if (cachedMsg.isRow == m.isRow && cachedMsg.rowOrColumnNumber == m.rowOrColumnNumber
                        && m.partNumber == cachedMsg.partNumber) {

                    byte[][][] responseData = custodyData2.get(0);
                    Message response = createMessage(m.id, Message.MSG_DATA, m.src,
                            m.messageTopicID,
                            responseData, m.isRow, m.rowOrColumnNumber,
                            m.partNumber, m.timestamp, m.ackId);

                    publishMessage(response, m.src, myPid);
                    System.out.println("Node " + myPid + " sent DATA response to Node " + m.src);
                    return;
                }
            }
        } else {
            System.out.println("Node " + myPid + " has no data to respond at time " +
                    CommonState.getTime());
        }
    }

    // Get node can create the whole row/col if it has half no of cells.
    // Currently it has 1 part out of *(assuming the row/col is divided into 8 equal
    // parts) so if requests 3 more parts it can reconstruct the whole row/col

    public void getRemainingRowColPart(Message m, int myPid) {
        int currentPart = m.partNumber;
        int n = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2; // Number of rows/columns per topic
        Random rand = new Random();

        // Prepare lists for all parts and random node indices
        List<Integer> allParts = new ArrayList<>();
        List<Integer> randomNodeIdx = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            if (i != currentPart) {
                allParts.add(i);
            }
            randomNodeIdx.add(i);
        }

        // Shuffle lists and select unique parts and node indices
        Collections.shuffle(allParts, rand);
        Collections.shuffle(randomNodeIdx, rand);

        List<Integer> uniquePartNumbers = allParts.subList(0, (n / 2) - 1);
        List<Integer> uniqueRandomNodeIdx = randomNodeIdx.subList(0, (n / 2) - 1);

        int isColumn = m.isRow ? 0 : 1;

        // Process three selected nodes
        for (int i = 0; i < 3; i++) {
            int partNumber = uniquePartNumbers.get(i);
            int nodeIndex = uniqueRandomNodeIdx.get(i);

            Node targetNode = topics.get(m.messageTopicID).topicMembers
                    .get((64 * isColumn) + (partNumber * 8) + nodeIndex);
            BigInteger destination = ((GossipSubProtocol) targetNode.getProtocol(gossipSubId)).nodeId;

            // Create and send IWANT request
            Message request = createMessage(-1, Message.MSG_IWANT, destination, m.messageTopicID, "",
                    m.isRow, m.rowOrColumnNumber, partNumber, 0, -1);
            publishMessage(request, m.src, myPid);

            // Set timeout for retransmission if no response is received
            Node src = CustomDistribution.networkNodes.get(this.nodeId);
            long latency = transport.getLatency(src, targetNode);
            EDSimulator.add(4 * latency, new peersim.GossipSub.Timeout(0, destination, request.id), src, gossipSubId);

            this.sentSeedingPartMsg.put(request.id, request);
        }
    }

    public void handleBlockProducerData(Message m, int myPid) {
        // System.out.println("Received data from block producer");
        // System.out.println("Recieved data from nodeID: "+ m.src +" to
        // "+this.getNodeId()+" ==="+ m.dest+" at time "+CommonState.getTime());
        if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId) {
            // System.out.println("Node ID: "+this.nodeId+" recieved the data from BP at
            // "+CommonState.getTime());
            messageArrivalTimeFromBP.add(CommonState.getTime());
            // System.out.println("Time in "+this.nodeId+" protocol is
            // "+CommonState.getTime());
            messageDelayTimeFromBP.add(CommonState.getTime() - m.timestamp);
            // System.out.println("RTT in "+this.nodeId+" is
            // "+(messageDelayTimeFromBP.get(0)));
            seedArrivalTimeStore.add(CommonState.getTime() - m.timestamp);
            if (!messageCache.containsKey(m.id)) // Checking the received data is already present
            {
                if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
                {
                    Long firstMessage = messageCache.keySet().iterator().next();
                    messageCache.remove(firstMessage); // Removing the first/oldest message if the cache is full
                }

                messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache

                if (m.partNumber != -1 && m.rowOrColumnNumber == -1 && distributionStrategy == 2) // This was sent using
                                                                                                  // the sharding method
                {
                    custodyData2.add((byte[][][]) m.body);
                    // messageCache.put(m.id,m);
                    // sending the metadata to all the nodes in this topic
                    // System.out.println(Arrays.deepToString((Object[]) m.body));
                    // System.out.println(messageCache.size()+" "+custodyData2.size()+" node
                    // "+this.nodeId+" "+CommonState.getTime());
                    Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID,
                            "", m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.ackId); // Temporalily
                                                                                                   // setting the dummy
                                                                                                   // .dest. It will set
                                                                                                   // correctly in the
                                                                                                   // sendMessageToPeers
                                                                                                   // Function
                    getRemainingRowColPart(m, myPid);
                    // gossipMessageToTopicNodes(responseWithMetaData,
                    // myPid,m.messageTopicID,this.nodeId);
                } else if (m.partNumber == -1 && m.rowOrColumnNumber != -1 && distributionStrategy == 1) // This was
                                                                                                         // sent using
                                                                                                         // the normal
                                                                                                         // method
                {
                    custodyData1.add((byte[][]) m.body);
                    // sending the metadata to all the nodes in this topic
                    Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID,
                            "", m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, m.ackId); // Temporalily
                                                                                                   // setting the dummy
                                                                                                   // .dest. It will set
                                                                                                   // correctly in the
                                                                                                   // sendMessageToPeers
                                                                                                   // Function
                    gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID, this.nodeId);
                    startSampling();
                }
                // System.out.println("NOde "+this.nodeId+" recieved data from:bp at
                // "+CommonState.getTime());
            }
        }
    }

    // This function handles the data received
    public void handleData(Message m, int myPid) {
        if (distributionStrategy == 1) {
            if (messageCache.containsKey(m.id)) // Checking if the m.id is already present;Like have we made the request
                                                // for. Because the request and response msg have the same id
            {
                messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache
                return;
            }
        }
        // System.out.println("Recieved data from nodeID: "+ m.src +" to
        // "+this.getNodeId()+" ==="+ m.dest+" at time "+CommonState.getTime());
        if (messageCache.containsKey(m.id)) // Checking if the m.id is already present;Like have we made the request
                                            // for. Because the request and response msg have the same id
        {

            // System.out.println("Node ID: "+this.nodeId+" recieved the data from BP at
            // "+CommonState.getTime());
            BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                    .getProtocol(gossipSubId))).nodeId;
            if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
            {
                for (Map.Entry<Long, Message> msg : messageCache.entrySet()) {
                    if (msg.getValue().src.equals(blockProposerId)) // Checking if the message is not sent by the block
                                                                    // proposer
                    {
                        continue;
                    }
                    Long firstMessage = messageCache.keySet().iterator().next();
                    messageCache.remove(msg.getKey()); // Removing the first/oldest message which is not sent by the
                                                       // block proposer if the cache is full which
                    break;
                }
            }
        }
        messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache
        if (sentSeedingPartMsg.containsKey(m.id)) {
            seedPartArrivalTimeFromPeer.add(CommonState.getTime());
            seedPartDelayTimeFromPeer.add(CommonState.getTime() - m.timestamp);
            seedPartArrivalTimeStore.add(CommonState.getTime() - m.timestamp);
            NoOfSeedPartsRecieved++;
            // System.out.println("Hurryy!!!! recieved the seeding part");
            sentSeedingPartMsg.remove(m.id);
            custodyData2.add((byte[][][]) m.body);
            partRequestCounter--;
        }

        if (distributionStrategy == 2 && partRequestCounter == 0) {
            // System.out.println("Yess");
            startSampling();
        }

        // Sending the message with data to all the peers/local mesh
        Message responseWithData = this.createMessage(m.id, Message.MSG_DATA, m.dest, m.messageTopicID, m.body, m.isRow,
                m.rowOrColumnNumber, m.partNumber, m.timestamp, -1); // Temporalily setting the dummy .dest. It will set
                                                                     // correctly in the sendMessageToPeers Function
        // sendMessageToPeers(responseWithData, myPid, m.messageTopicID, m.src);

        // sending the metadata to all the nodes in this topic
        Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "",
                m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp, -1); // Temporalily setting the dummy .dest. It
                                                                              // will set correctly in the
                                                                              // sendMessageToPeers Function
        // gossipMessageToTopicNodes(responseWithMetaData, myPid,
        // m.messageTopicID,this.nodeId);
    }

    // Function the create the Message instance
    private Message createMessage(long id, int type, BigInteger dest, String topicID, Object body, boolean isRow,
            int RowOrColNum, int partNum, long timeStamp, long ackid) {
        Message msg;
        if (id == -1) // Create a new message with new message Id
        {
            msg = new Message(type, isRow, RowOrColNum, partNum, ackid);
            msg.timestamp = CommonState.getTime(); // Getting the current time/ time at which the message was created
        } else {
            msg = new Message(id, type, isRow, RowOrColNum, partNum, ackid);
            msg.timestamp = timeStamp;
        }
        msg.src = this.nodeId;
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
    public void sampleDataRequest() {
        // System.out.println("This node is sampling " + this.nodeId);
        Random random = new Random();

        int rowOrColDecider = random.nextInt(2);
        int randomSampleIndex = random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512)); // 0 to 262143 inclusive
                                                                                                // (total numbers of
                                                                                                // rows/col = 512;each
                                                                                                // contains 512 to
                                                                                                // samples;512*512=262144)
        int SamplingIdx = randomSampleIndex; // get sample a particular idx at a given row/col
        int rowOrColNo = randomSampleIndex;
        int topicNo = rowOrColNo / ((Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC", 16) / 2));
        Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo + 1));
        if (topicToSubscribe == null) {
            // System.out.println("error");

            return;
        }
        this.subscribeTopic(topicToSubscribe); // Subscribe to the topic to which the row/col to be sampled belongs to

        int nodeTopicPosition = (8 * rowOrColDecider) + ((rowOrColNo / 64));
        int nodeIndex = 0;
        if (distributionStrategy == 1) {
            int sampleHolderNodeIdx = random.nextInt(8); // Random number generated to select which the node to send the
                                                         // request as 8 nodes hold the same rows/cols
            nodeIndex = ((rowOrColNo % 8) * 8) + sampleHolderNodeIdx * 1; // As 8 nodes hold the same rows/cols.
        } else if (distributionStrategy == 2) {
            nodeIndex = random.nextInt(64); // As 64 nodes in the topic will hold 8 rows/cols as they willl reconstruct
                                            // the whole row/col after reciveing the parts. So you can sample any node
        }
        // System.out.println(nodeTopicPosition);
        BigInteger destId = null;
        if (rowOrColDecider == 0) { // Row sampling
            // Nodes 0-7 handle rows
            // destId = ((Old_GossipSubProtocol)
            // (topicToSubscribe.topicMembers.get(nodeIndex)
            // .getProtocol(gossipSubId))).nodeId;

            if (nodeIndex < topicToSubscribe.topicMembers.size()) {
                Node targetNode = topicToSubscribe.topicMembers.get(nodeIndex);
                if (targetNode != null) {
                    Protocol protocol = targetNode.getProtocol(gossipSubId);
                    if (protocol instanceof GossipSubProtocol) {
                        destId = ((GossipSubProtocol) protocol).nodeId;
                    } else {
                        System.err.println(
                                "Error: Protocol at index " + nodeIndex + " is not of type GossipSubProtocol");
                        return;
                    }
                } else {
                    System.err.println("Error: Null node at index " + nodeIndex);
                    return;
                }

            } else {
                System.err.println("Error: nodeIndex " + nodeIndex + " out of bounds. topicMembers size: "
                        + topicToSubscribe.topicMembers.size());
                return;
            }

        } else

        { // Column sampling
          // Nodes 8-15 handle columns
          // destId = ((Old_GossipSubProtocol)
          // (topicToSubscribe.topicMembers.get(nodeIndex + 64)
          // .getProtocol(gossipSubId))).nodeId;
            if (nodeIndex + 64 < topicToSubscribe.topicMembers.size()) {
                Node targetNode = topicToSubscribe.topicMembers.get(nodeIndex + 64);
                if (targetNode != null) {
                    Protocol protocol = targetNode.getProtocol(gossipSubId);
                    if (protocol instanceof GossipSubProtocol) {
                        destId = ((GossipSubProtocol) protocol).nodeId;
                    } else {
                        System.err.println("Error: Protocol at index " + (nodeIndex + 64)
                                + " is not of type GossipSubProtocol");
                        return;
                    }
                } else {
                    System.err.println("Error: Null node at index " + (nodeIndex + 64));
                    return;
                }
            } else {
                System.err.println("Error: nodeIndex + 64 (" + (nodeIndex + 64) + ") out of bounds. topicMembers size: "
                        + topicToSubscribe.topicMembers.size());
                return;
            }

        }
        if (destId == this.nodeId) {
            // System.out.println("cant sample bcz destId is equal to current nodeId");
            // System.out.println(destId+" "+this.nodeId);
            sampleDataRequest();
            return;
        }

        Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID,
                Integer.toString(SamplingIdx), rowOrColDecider == 0, rowOrColNo, -1, 0, -1);// Need to change/calculate
                                                                                            // the part the partNo. Just
                                                                                            // added some random value
                                                                                            // atm

        this.sentMsg.put(sampleReqMes.id, sampleReqMes);
        NoOfSampleRequestsSent++;
        // System.out.println("Number of sample request sent "+NoOfSampleRequestsSent);
        this.publishMessage(sampleReqMes, destId, gossipSubId);

        // Setting the timeout. This will node will send the sample request again if it
        // doesn't recieve the sample within given time
        peersim.GossipSub.Timeout t = new peersim.GossipSub.Timeout(1, destId, sampleReqMes.id);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(sampleReqMes.dest);
        long latency = transport.getLatency(src, dest);

        EDSimulator.add(4 * latency, t, src, gossipSubId); // set delay = 2*RTT

    }

    // Repeat this 75 times
    // Make sure that the sampling is not done for the same
    // samples---------------------------------------
    public void startSampling() {
        for (int i = 0; i < 75; i++) {
            sampleDataRequest();
        }
    }

    public void handleSampleRequest(Message m, int myPid) // Sending the requested sample data if the current node has
                                                          // it
    {
        // System.out.println("Request received for sample");
        boolean requestedRow = m.isRow;
        int rowOrColNumb = m.rowOrColumnNumber;
        int idx = Integer.parseInt((String) m.body);
        // System.out.println("This node recieved a sampling request "+this.nodeId);
        // System.out.println(idx);
        // System.out.println("Message cashe size "+messageCache.size()+" this node id
        // "+this.nodeId+ " time "+CommonState.getTime());
        BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId))).nodeId;

        for (Map.Entry<Long, Message> msg : messageCache.entrySet()) {
            Message curMessage = msg.getValue();
            if (distributionStrategy == 1) {
                // System.out.println("*****");
                // if(curMessage.src.compareTo(blockProposerId)==0)

                // System.out.print(curMessage.isRow+" "+m.isRow+" ");
                // System.out.print(curMessage.rowOrColumnNumber+" "+m.rowOrColumnNumber+" ");
                // System.out.println(curMessage.messageTopicID+" "+m.messageTopicID);
                // System.out.println(this.nodeId);
                // System.out.println("*****");
                if (curMessage.src.equals(blockProposerId) && curMessage.isRow == m.isRow
                        && curMessage.rowOrColumnNumber == m.rowOrColumnNumber) // Checking if the message is not sent
                                                                                // by the block proposer
                {
                    // System.out.println("yes inside");
                    // System.out.println("nodeID "+this.nodeId);
                    byte[][] data = (byte[][]) curMessage.body; // If the number stored at this index is greater than
                                                                // 127 it will store the negative value as byte is type
                                                                // is signed meaning the values range in 127 to -127
                    byte sampleDataResp = data[0][idx];
                    Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, m.src,
                            m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber,
                            m.partNumber, m.timestamp, m.ackId);
                    this.publishMessage(sampleResponse, m.src, gossipSubId);
                    return;
                } else if (!custodyData1.isEmpty() && curMessage.body == custodyData1.get(0)) {
                    // System.out.print("Received sampling request "+this.nodeId+"row/col =
                    // "+curMessage.rowOrColumnNumber+" TOPIC NUMBER "+(curMessage.messageTopicID)+"
                    // is row ");
                    // System.out.println(curMessage.isRow==true);

                }

            }
        }
        if (distributionStrategy == 2) {
            // System.out.println("yesssss");
            // if (curMessage.partNumber != -1 && curMessage.rowOrColumnNumber == -1 &&
            // curMessage.rowOrColumnNumber == m.rowOrColumnNumber && curMessage.isRow ==
            // m.isRow) {

            if (custodyData2.size() == 4) // checking if the node recieved half the row/cow (4 out of 8 parts)
            {
                byte[][][] data = (byte[][][]) custodyData2.get(0);// Currently we are just sending random sample
                                                                   // response as we are not forming the whole row/col
                                                                   // over here but in reality the will form the whole
                                                                   // row/col if it has 50% cells
                byte sampleDataResp = data[0][0][0];
                Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, m.src, m.messageTopicID,
                        Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,
                        m.ackId);
                this.publishMessage(sampleResponse, m.src, gossipSubId);
            } else {
                // System.out.println(this.nodeId+ " No!!!!!!");
            }
            // }

        }
        // }

    }

    public void handleSampleResponse(Message m, int myPid) {

        sampleArrivalTime.add(CommonState.getTime());
        sampleDelayTime.add(CommonState.getTime() - m.timestamp);
        samplingRTTTimeStore.add(CommonState.getTime());
        NoOfSamplesRecieved++;
        if (m.body != null) {
            // System.out.println("Yahoooo! got the sample data from node "+m.src);
            // System.out.println(m.body);
        }
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
                    // System.out.println("Received data from " + m.src + " to " + this.nodeId + "
                    // For topic "+ m.messageTopicID + " " + i++ + " " + m.id);
                    // System.out.println();
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
                if (distributionStrategy == 1) {
                    blockProducer();
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
                } else {
                    // System.out.println("Already removed");
                }
                break;

            case peersim.GossipSub.Timeout.TIMEOUT: // timeout
                peersim.GossipSub.Timeout t = (Timeout) event;
                if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
                    this.NoOfSampleRequestsSent++;
                    // System.out.println("Sample request message TimeOut. Sample not recieved. Sent
                    // a sample request again");
                    sampleRequestUnsuccessful++;
                    // System.out.println("sample request unsuccessfull "+
                    // sampleRequestUnsuccessful);
                    // Send sample request again
                    Message sampleMsgSent = sentMsg.get(t.msgID);
                    sentMsg.remove(t.msgID); // remove form sentMsg
                    Message msgToResend = this.createMessage(-1, sampleMsgSent.type, sampleMsgSent.dest,
                            sampleMsgSent.messageTopicID, sampleMsgSent.body, sampleMsgSent.isRow,
                            sampleMsgSent.rowOrColumnNumber, sampleMsgSent.partNumber, 0, sampleMsgSent.ackId);
                    sentMsg.put(msgToResend.id, msgToResend);
                    // System.out.println("Sent message again to "+msgToResend.dest);
                    publishMessage(msgToResend, msgToResend.dest, myPid);

                } else if (sentSeedingPartMsg.containsKey(t.msgID)) {

                    Message sentMsg = sentSeedingPartMsg.get(t.msgID);
                    sentSeedingPartMsg.remove(t.msgID);
                    Message msgToResend = this.createMessage(-1, sentMsg.type, sentMsg.dest, sentMsg.messageTopicID,
                            sentMsg.body, sentMsg.isRow, sentMsg.rowOrColumnNumber, sentMsg.partNumber, 0,
                            sentMsg.ackId);
                    sentSeedingPartMsg.put(msgToResend.id, msgToResend);
                    // System.out.println("Sending part request to node "+msgToResend.dest+ " for
                    // part "+msgToResend.partNumber+" row/col "+msgToResend.rowOrColumnNumber + "
                    // by node "+this.nodeId+msgToResend.isRow);
                    publishMessage(msgToResend, msgToResend.dest, myPid);
                }
                break;
            case Message.MSG_EMPTY:
                break;
            // TO DO

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

    // Trying to implement
    // The validator can recontruct the row/columns if it has 50%(256 cells) of each
    // row/col
    // The BP does send the whole row/col divided into parts, where number of parts
    // equals to number of rows/cols in a topic.
    // So the first 8 nodes in the topic will hold the row parts and the next 8
    // nodes will hold the column parts
    // So the first node will fold the 1st parts of all the 8 rows which are divided
    // into 8 parts

    private void blockProducer() {
        // System.out.println(Configuration.getInt("NUMBER_COPIES_DISTRIBUTED"));
        int rowNumber = 0;
        int columnNumber = 0;

        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS")); // Creating
                                                                                                                // a
                                                                                                                // block

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId)); // Get the protocol instance of the block proposer

        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            int cnt = 0;
            int nodeCounter = 0;
            // System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {
                if (cnt < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2) // Allocating first 8 nodes in the
                                                                                     // topic with rows if
                                                                                     // NUMBER_OF_ROWSCOLS_IN_A_TOPIC=16
                {
                    byte[][] rowToSend = b.getRowData(rowNumber); // row to be sent

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    // System.out.println("Row number is "+rowNumber+" node ID "+destID+" for topic
                    // "+topicEntry.getValue().topicID);
                    Message newMessage = new Message(3, rowToSend, true, rowNumber, -1, -1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                    newMessage.dest = destID;

                    // System.out.println("Message with id: " + newMessage.id + " belongs to " +
                    // newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to
                                                                          // validator node
                    nodeCounter++;
                    if (nodeCounter % 8 == 0) {
                        rowNumber++;
                        cnt++;
                    }
                } else // Allocating the rest 8 nodes in the topic with columns
                {
                    if (columnNumber == 512) // added this as it causing indexoverflow error as there is a bug in
                                             // row/col allocation
                    {
                        continue;
                    }
                    byte[][] colToSend = b.getColumnData(columnNumber);

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    // System.out.println("Col number is "+columnNumber+" node ID "+destID+" for
                    // topic "+topicEntry.getValue().topicID);
                    Message newMessage = new Message(3, colToSend, false, columnNumber, -1, -1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    newMessage.dest = destID;

                    this.publishMessage(newMessage, destID, gossipSubId);
                    nodeCounter++;
                    if (nodeCounter % 8 == 0) {
                        columnNumber++;
                        cnt++;
                    }
                }
            }
        }
        System.out.println(rowNumber);
        System.out.println(columnNumber);
        System.out.println("*********Block proposer has sent the messages ********");
    }

    private void shardingBasedDistribution() {
        int numberOfDivisions = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2;// Equals to the number of
                                                                                          // rows/columns per topic
        int partSize = Configuration.getInt("NUMBER_OF_ROWS") / numberOfDivisions;

        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS")); // Creating
                                                                                                                // a
                                                                                                                // block
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId)); // Get the protocol instance of the block proposer
        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());

        int curTopicNumber = 0;
        int cnt;
        int rowPartNo;
        int colPartNo;
        for (Map.Entry<String, Topic> topicEntry : topics.entrySet()) // Looping over all the topics
        {
            cnt = 0;
            rowPartNo = 0;
            colPartNo = 0;
            int nodeCounter = 0;

            // System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {

                if (cnt < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2) // Allocating first 8 nodes in the
                                                                                     // topic with rows if
                                                                                     // NUMBER_OF_ROWSCOLS_IN_A_TOPIC=16
                {
                    byte[][][] rowsToSend = new byte[8][][];
                    int start = rowPartNo * partSize;
                    int end = start + partSize;
                    // System.out.println("*******");
                    for (int i = 0; i < numberOfDivisions; i++) {
                        rowsToSend[i] = Arrays.copyOfRange(b.getRowData(curTopicNumber * 8 + i), start, end);
                    }
                    // System.out.println(Arrays.toString(b.getRowData(curTopicNumber * 8 + 1)));
                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    Message newMessage = new Message(3, rowsToSend, true, -1, rowPartNo, -1);
                    // System.out.println("Row parts-"+rowPart+" sent to "+destID+ " from BP "+"
                    // "+Arrays.deepToString(rowsToSend));
                    // System.out.println("*****");
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    newMessage.dest = destID;

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to
                                                                          // validator node

                    nodeCounter++;
                    if (nodeCounter % 8 == 0) {
                        rowPartNo++;
                        cnt++;
                    }
                } else // Allocating the rest 8 nodes in the topic with columns
                {
                    byte[][][] colsToSend = new byte[8][][];
                    int start = colPartNo * partSize;
                    int end = start + partSize;
                    for (int i = 0; i < numberOfDivisions; i++) {
                        colsToSend[i] = Arrays.copyOfRange(b.getColumnData(curTopicNumber * 8 + i), start, end);
                    }

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    Message newMessage = new Message(3, colsToSend, false, -1, colPartNo, -1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    newMessage.dest = destID;

                    this.publishMessage(newMessage, destID, gossipSubId);

                    nodeCounter++;
                    if (nodeCounter % 8 == 0) {
                        cnt++;
                        colPartNo++;
                    }
                }
            }
            curTopicNumber++;
        }

        System.out.println("*********Block proposer has sent the messages ********");
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
