package peersim.gossipsub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.gossipsub.Timeout;
import peersim.transport.UnreliableTransport;
import peersim.util.IncrementalStats;

import java.math.BigInteger;
import java.util.*;

import static peersim.gossipsub.CustomDistribution.topics;

public class GossipSubProtocol implements Cloneable, EDProtocol {
    // Maximum size of messageCashe
    public static final int MESSAGE_CACHE_SIZE = 1024; // Just took a random number atm. May need to update it--------------------------------
    private static final String PAR_TRANSPORT = "transport";
    private static String prefix = null;
    public BigInteger nodeId;
    //Stats for messages delivered from the block proposer(BP)
    public IncrementalStats seedArrivalTimeStore = new IncrementalStats(); // The time at which the seeds(rows or columns) arrived from the block proposer
    public ArrayList<Long> messageArrivalTimeFromBP; // It stores all the time stamp at which the message with the data was arrivied from the block proposer
    public ArrayList<Long> messageDelayTimeFromBP; // It stores all the difference bt. the message arrival time(the time at which the message was received) and the time the message was created/sent from the block proposer. So in short it gives the time it took for the message to reach the destination
    //Stats for seeding part messages successfully recieved after sending the request from the topic peers
    public IncrementalStats seedPartArrivalTimeStore = new IncrementalStats(); // The time at which the seed(rows or columns) parts arrived from the peers
    public ArrayList<Long> seedPartArrivalTimeFromPeer; // It stores all the time stamp at which the seed part with the data was arrivied from the peer
    public ArrayList<Long> seedPartDelayTimeFromPeer; // It stores all the difference bt. the message arrival time(the time at which the message was received) and the time the message was sent from the peer. So in short it gives the time it took for the message to reach the destination
    //Stats for sampling messages successfully recieved after sending the request
    public IncrementalStats samplingRTTTimeStore = new IncrementalStats();
    public ArrayList<Long> sampleArrivalTime; // It stores all the time stamp at which the message with the data was arrivied after the current node sent sample request
    public ArrayList<Long> sampleDelayTime; // It stores all the difference bt. the sample arrival time(the time at which the sample was received) and the time the message(sample request was sent) was created/sent from the block proposer. So it the net time from this node sending the sample the request, the sample holder recieving and sending the sample and the sample arriving to the current node
    public int sampleRequestUnsuccessful; // It stores the number of sample request that timed out and had to send the sample request again
    public int NoOfSampleRequestsSent; // It stores the total number of sample requests sent
    public int NoOfSamplesRecieved; // It stores the total number of samples recieved
    public int NoOfSeedPartsRecieved; // It stores the total number of seed parts recieved(Only in the case of sharded proposer distribution)
    protected int degree;
    // Maps each topic the current node is a member to a set of peer IDs that are part of its local mesh.
    // This represents the direct connections in the local mesh.
    protected Map<String, Set<BigInteger>> localMesh;

    // List all the topic for which this node is a member. Mapping of topics to list of all the members in a given topic
    protected Map<String, Set<BigInteger>> topicNodes; // <Topic Name, Arraylist<All the members in this topic>>
    int partRequestCounter = 3; // It stores the number part requests made during the seeding phase
    //This arraylist contains the rows/cols it was intially given by the block proposer to custody
    //Logic 1: Right now the node will forward the initial data to all the peers in the that topic meaning that eventually all the peers in that topic will hold that data as well.
    //Logic 2: To avoid to forward the initial data received from the data proposer to the nodes in the topic we can data that data to the custodyData arraylist and not to the message cashe and hence not forward that message to our peers. Just send the message with the metadata.
    int distributionStrategy;
    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;
    private int degreeLow;
    private int degreeHigh;
    private int i = 0;
    // The message cache stores recent messages Only add messages with data All the messages(IHAVE,IWANT,DATA) have the same message ID for the same data, so it is easy to compare/store in messageCache
    private LinkedHashMap<Long, Message> messageCache; // <MessageID,Message>
    // Stores the topic ids of the which it is associated with
    private Set<Topic> subscribedTopics;
    private ArrayList<byte[][]> custodyData1; // If the distributionStrategy=1
    private ArrayList<byte[][][]> custodyData2; // If the distributionStrategy=2
    private int interfaceBandwidth;
    private int blockProducerBandwidth;
    private long lastMessageTransmissionTime; // The transmission time of the new/latest message added to the queue(transmission time for the last element in the messageQueue)
    private ArrayList<Message> messageQueue = new ArrayList<>(); // arraylist to store the messages to be sent from this node
    private ArrayList<Long> messageTransmissionDelayQueue = new ArrayList<>(); // arraylist to transmission delay time for the message to be sent
    //    trace the sample request message sent for timeout purpose
    private TreeMap<Long, Message> sentMsg;
    private TreeMap<Long, Message> sentSeedingPartMsg;
    int randomwSampleCounter; //This variable will store a random number when initially initialized  and decremented everytime this node gets the IHAVE message. When it becomes 0 it will send IWANT message to sending node. It will only be useful in distributionStragerty 1 because 2 it eventually recieves 8 row/cols and 3 it recieves 2 rows/cols as the validator node has to sample 2 complete rows/cols and 75 samples

    public GossipSubProtocol(String prefix) {
        this.degree = 8;
        this.degreeLow = 6;
        this.degreeHigh = 14;

        this.localMesh = new HashMap<>();

        this.messageCache = new LinkedHashMap<>();

        this.subscribedTopics = new HashSet<>();

        this.topicNodes = new HashMap<>();

        this.nodeId = null;

        this.custodyData1 = new ArrayList<>();
        this.custodyData2 = new ArrayList<>();

        this.interfaceBandwidth = Configuration.getInt("INTERFACE_BANDWIDTH", 100000000);
        this.blockProducerBandwidth = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1000000000);

        GossipSubProtocol.prefix = prefix;

        tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

        this.messageArrivalTimeFromBP = new ArrayList<>();
        this.messageDelayTimeFromBP = new ArrayList<>();

        this.seedPartArrivalTimeFromPeer = new ArrayList<>();
        this.seedPartDelayTimeFromPeer = new ArrayList<>();

        this.sampleArrivalTime = new ArrayList<>();
        this.sampleDelayTime = new ArrayList<>();

        this.sampleRequestUnsuccessful = 0;
        this.NoOfSampleRequestsSent = 0;
        this.NoOfSamplesRecieved = 0;
        this.NoOfSeedPartsRecieved =0;

        sentMsg = new TreeMap<Long, Message>();
        sentSeedingPartMsg = new TreeMap<Long, Message>();

        distributionStrategy = Configuration.getInt("DISTRIBUTION_STRATEGY");

        Random rnd = new Random();
        randomwSampleCounter = rnd.nextInt(Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC")-1);
//        System.out.println("this node's"+this.nodeId+" initial counter "+randomwSampleCounter);
    }

    public Object clone() {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        return cln;
    }

    // Calculating the size of the message in bytes
    private int calculateMessageSize(Message message) {
        int size = 0;
        size += Integer.BYTES; //Message Type
        size += Long.BYTES; // Message ID
        size += message.src.toString().getBytes().length; // Source ID size
        size += message.dest.toString().getBytes().length; // Destination ID size
        if (message.messageTopicID != null) {
            size += message.messageTopicID.getBytes().length; // Topic ID size
        }
        if (message.body != null) {
            if (message.body instanceof byte[][]) { // Size of 2D array
                byte[][] bodyArray = (byte[][]) message.body;
                for (byte[] row : bodyArray) {
                    size += row.length;
                }
            } else if (message.body instanceof String) {
                size += ((String) message.body).getBytes().length; // Size of the body (String)
            } else if (message.body instanceof byte[][][]) { // Size of the 3D array
                byte[][][] bodyArray = (byte[][][]) message.body;
                for (byte[][] matrix : bodyArray) {
                    for (byte[] row : matrix) {
                        size += row.length;
                    }
                }
            }
        }
        size += 1; // For isRow flag
        size += Integer.BYTES; // For rowOrColumnNumber
        size += Integer.BYTES; // For partNumber
        size += Long.BYTES; // For the timestamp
//        System.out.println("size "+size);
        return size;
    }

    // Topic subscription function. Used to subscribe to a particular topic
    public void subscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            return;
        }
        subscribedTopics.add(topic);
        // System.out.println("Node with ID: "+this.getNodeId()+" Subscribed to topic: "+topic.topicID);
        localMesh.put(topic.topicID, new HashSet<>());
    }

    // Topic unsubscription function
    public void unsubscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            subscribedTopics.remove(topic);
            localMesh.remove(topic.topicID);
            return;
        }
    }

    //Function to add a topic to which this node is subsribed and its member
    public void setTopicMembersList(String topicName, Set<BigInteger> members) {
        topicNodes.put(topicName, members);
    }

    //Function to send the message in the network
    public void publishMessage(Message m, BigInteger destId, int myPid) {
        int bandwidth = 0;
        if (this.nodeId == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId) {
            bandwidth = blockProducerBandwidth;
        } else {
            bandwidth = interfaceBandwidth;
        }
        this.messageCache.put(m.id, m);

        long currentTime = 0;
        long queuingDelay = 0;

        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        long latency = transport.getLatency(src, dest); // Calculate message delay/propogation delay

        if (messageQueue.isEmpty()) {
            queuingDelay = 0;
        } else {
            while (!messageTransmissionDelayQueue.isEmpty() && CommonState.getTime() > messageTransmissionDelayQueue.get(0)) {
                messageQueue.remove(0);
                messageTransmissionDelayQueue.remove(0);
            }
            if (!messageQueue.isEmpty()) {
//                System.out.println("Delay!!!!");
                queuingDelay = messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1) - CommonState.getTime();
            } else {
                queuingDelay = 0;
            }
        }
        int messageSize = calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / (double) bandwidth); // Use Math.ceil to round up the transmission time so that even fractional transmission time are rounded up to the next whole unit for accurate message transmission.
        long propogationDelay = latency; // Do we need add propogation delay when sceduling the event??
        long totalDelay = queuingDelay + transmissionDelay + propogationDelay;
        if (totalDelay != 0) {
//        System.out.println("Delay at src node "+this.nodeId+ "to reciving node "+ m.dest+ " Queuing delay "+queuingDelay+" progation delay "+propogationDelay+" transmission delay "+transmissionDelay+" sceduled at "+CommonState.getTime()+totalDelay);
        }
        EDSimulator.add(totalDelay, m, dest, myPid);
        long waitUntilMessageSent = CommonState.getTime() + transmissionDelay;
        messageQueue.add(m);
        messageTransmissionDelayQueue.add(waitUntilMessageSent);
        lastMessageTransmissionTime = waitUntilMessageSent;

        //Use transport protocol to send the message instead of EDSimulator. as transport protocol eventually calls EDSimulator. Just use EDSimulator in the initializer where the block proposer sends the rows/cols to the validator nodes
    }

    // Send Message to all nodes in topic
    public void sendMessageToTopicNodes(Message m, int myPid, String topicID, Map<String, Set<BigInteger>> nodesInTopic, BigInteger messageSender) {
        Set<BigInteger> topicNodes = nodesInTopic.get(topicID);

        for (BigInteger peerId : topicNodes) {
            if (peerId == messageSender) // Condition to avoiding sending the message back to the node who forwarded the message to us
            {
                // System.out.println("Dont send the message as we recieved this message from it");
                continue;
            }

            Message newMessage = this.createMessage(-1,m.type,m.dest,m.messageTopicID,m.body,m.isRow,m.rowOrColumnNumber,m.partNumber,m.timestamp,m.id);
            newMessage.dest = peerId;
            publishMessage(newMessage, peerId, myPid);
        }
    }

    // Send message to all the peers/local mesh in the given topic
    public void sendMessageToPeers(Message m, int myPid, String topicID, BigInteger avoid) {
        if (this.localMesh.get(topicID).isEmpty()) {
            System.out.println("The local mesh for node: " + this.nodeId + " is empty");
            return;
            // System.out.println("Called sendMessageToPeers in " + this.nodeId);
        }
        sendMessageToTopicNodes(m, myPid, topicID, this.localMesh, avoid);
    }

    // Send the Metadata all the nodes/global mesh in the topic
    // Implement that a random no of nodes are selected for gossiping-------------------
    public void gossipMessageToTopicNodes(Message m, int myPid, String topicID, BigInteger avd) {
        sendMessageToTopicNodes(m, myPid, topicID, this.topicNodes, avd);
    }

    //Function to handle the recieved IHave messages
    public void handleIHave(Message m, int myPid) {
        if (messageCache.containsKey(m.id))// Message already present in the cache / already seen
        {
//            randomwSampleCounter--;
            return;
        } else // Do we need to do this---------- (Might implememnt a randomly; whether to make a request for the data or not)-------
        {
            if(distributionStrategy==1 && randomwSampleCounter==0) {
//                System.out.println("Sent I want req");
                // Create IWANT Request Message to request the data
                // Keeping the id of the new message same as the IHave message
//                System.out.println("This node sent Iwant req "+this.nodeId);
                for(Message msg:messageCache.values())
                {
                    if(msg.isRow==m.isRow && msg.rowOrColumnNumber==m.rowOrColumnNumber)//If we are holding the same row/col then don't send IWANT request
                    {
                        return;
                    }
                }
                Message request = this.createMessage(m.id, Message.MSG_IWANT, m.src, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,m.ackId);

                publishMessage(request, m.src, myPid);
            }
        }
        randomwSampleCounter--;
    }

    // Send data to the node who requested/sent IWant message
    // Compare the Iwant messages with the requested data message and send that
    public void handleIWANT(Message m, int myPid) {
        if (distributionStrategy==1 && messageCache.containsKey(m.ackId))
        {
//            System.out.println("Recieved I want req");
            // Create DATA/response Message
            Message response = this.createMessage(m.id, Message.MSG_DATA, m.src, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber,m.partNumber,m.timestamp,m.ackId);
            // send data/response to IWANT Request
            publishMessage(response, m.src, myPid);
            return;
        }
        if (!custodyData2.isEmpty()) {
            // Create DATA/response Message
//            System.out.println("this node is full "+this.nodeId+" at "+CommonState.getTime());
            for(Message msg:messageCache.values())
            {
                if(msg.isRow==m.isRow && msg.rowOrColumnNumber==m.rowOrColumnNumber && m.partNumber==msg.partNumber)//If we are holding the same row/col then don't send IWANT request
                {
                    byte[][][] responseData = custodyData2.get(0);
                    Message response = this.createMessage(m.id, Message.MSG_DATA, m.src, m.messageTopicID, responseData, m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,m.ackId);

                    // send data/response to IWANT Request
                    publishMessage(response, m.src, myPid);
                    return;
                }
            }
//            System.out.println("I only have "+this.nodeId+" part "+messageCache.values().iterator().next().partNumber+" row/col "+messageCache.values().iterator().next().rowOrColumnNumber+" "+messageCache.values().iterator().next().src+" "+messageCache.values().iterator().next().isRow);

        } else {
//            System.out.println("this node is empty "+this.nodeId+" at "+CommonState.getTime());
        }
    }

    //Get node can create the whole row/col if it has half no of cells.
    //Currently it has 1 part out of *(assuming the row/col is divided into 8 equal parts) so if requests 3 more parts it can reconstruct the whole row/col
    public void getRemaingRowColPart(Message m, int myPid) {
//        System.out.println("In "+this.nodeId);
        int currentPart = m.partNumber;
        Random rand = new Random();
        int n = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2;// Equals to the number of rows/columns per topic
        int randomNumber = rand.nextInt(n);

        List<Integer> allParts = new ArrayList<>();
        List<Integer> randomNodeIdx = new ArrayList<>();
        for (int i = 0; i < n; i++) { // The arraylist will store all the partNo apart from the one the currentNode has
            if (i != currentPart) {
                allParts.add(i);
            }
            randomNodeIdx.add(i);
        }
        Collections.shuffle(allParts, rand); // Shuffle all the parts
        List<Integer> uniquePartNumbers = allParts.subList(0, (n/2)-1); // Get any 3(if n=8) parts to send the request
        List<Integer> uniqueRandomNodeIdx = randomNodeIdx.subList(0, (n/2)-1); // Get any 3(if n=8) parts to send the request

        Node n1, n2, n3;
        Message requestPart1, requestPart2, requestPart3;
        BigInteger dest1, dest2, dest3;
        if (m.isRow) {
//            System.out.println("row");
            n1 = topics.get(m.messageTopicID).topicMembers.get((uniquePartNumbers.get(0)*8)+uniqueRandomNodeIdx.get(0));
            dest1 = ((GossipSubProtocol) (n1.getProtocol(gossipSubId))).nodeId;
            requestPart1 = this.createMessage(-1, Message.MSG_IWANT, dest1, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, uniquePartNumbers.get(0), 0,-1);

            n2 = topics.get(m.messageTopicID).topicMembers.get((uniquePartNumbers.get(1)*8)+uniqueRandomNodeIdx.get(1));
            dest2 = ((GossipSubProtocol) (n2.getProtocol(gossipSubId))).nodeId;
            requestPart2 = this.createMessage(-1, Message.MSG_IWANT, dest2, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, uniquePartNumbers.get(1), 0,-1);

            n3 = topics.get(m.messageTopicID).topicMembers.get((uniquePartNumbers.get(2)*8)+uniqueRandomNodeIdx.get(2));
            dest3 = ((GossipSubProtocol) (n3.getProtocol(gossipSubId))).nodeId;
            requestPart3 = this.createMessage(-1, Message.MSG_IWANT, dest3, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, uniquePartNumbers.get(2), 0,-1);

        } else {
//            System.out.println("col");
            n1 = topics.get(m.messageTopicID).topicMembers.get(64 + (uniquePartNumbers.get(0)*8)+uniqueRandomNodeIdx.get(0));
            dest1 = ((GossipSubProtocol) (n1.getProtocol(gossipSubId))).nodeId;
            requestPart1 = this.createMessage(-1, Message.MSG_IWANT, dest1, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, uniquePartNumbers.get(0), 0,-1);

            n2 = topics.get(m.messageTopicID).topicMembers.get(64 + (uniquePartNumbers.get(1)*8)+uniqueRandomNodeIdx.get(1));
            dest2 = ((GossipSubProtocol) (n2.getProtocol(gossipSubId))).nodeId;
            requestPart2 = this.createMessage(-1, Message.MSG_IWANT, dest2, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, uniquePartNumbers.get(1), 0,-1);

            n3 = topics.get(m.messageTopicID).topicMembers.get(64 + (uniquePartNumbers.get(2)*8)+uniqueRandomNodeIdx.get(2));
            dest3 = ((GossipSubProtocol) (n3.getProtocol(gossipSubId))).nodeId;
            requestPart3 = this.createMessage(-1, Message.MSG_IWANT, dest3, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, uniquePartNumbers.get(2), 0,-1);
        }
        publishMessage(requestPart1, m.src, myPid);
        publishMessage(requestPart2, m.src, myPid);
        publishMessage(requestPart3, m.src, myPid);

        //Setting the timeout. This will node will send the  request again if it doesn't recieve the part within given time
        Node src = CustomDistribution.networkNodes.get(this.nodeId);

        peersim.gossipsub.Timeout t1 = new peersim.gossipsub.Timeout(0, dest1, requestPart1.id);
        long lat1 = transport.getLatency(src, n1);
        EDSimulator.add(4 * lat1, t1, src, gossipSubId); // set delay = 2*RTT
        this.sentSeedingPartMsg.put(requestPart1.id, requestPart1);

        peersim.gossipsub.Timeout t2 = new peersim.gossipsub.Timeout(0, dest2, requestPart2.id);
        long lat2 = transport.getLatency(src, n2);
        EDSimulator.add(4 * lat2, t2, src, gossipSubId); // set delay = 2*RTT
        this.sentSeedingPartMsg.put(requestPart2.id, requestPart2);

        peersim.gossipsub.Timeout t3 = new peersim.gossipsub.Timeout(0, dest3, requestPart3.id);
        long lat3 = transport.getLatency(src, n3);
        EDSimulator.add(4 * lat3, t3, src, gossipSubId); // set delay = 2*RTT
        this.sentSeedingPartMsg.put(requestPart3.id, requestPart3);
    }

    public void handleBlockProducerData(Message m, int myPid) {
//        System.out.println("Received data from block producer");
//        System.out.println("Recieved data from nodeID: "+ m.src +" to "+this.getNodeId()+" ==="+ m.dest+" at time "+CommonState.getTime());
        if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId) {
//            System.out.println("Node ID: "+this.nodeId+" recieved the data from BP at "+CommonState.getTime());
            messageArrivalTimeFromBP.add(CommonState.getTime());
//            System.out.println("Time in "+this.nodeId+" protocol is "+CommonState.getTime());
            messageDelayTimeFromBP.add(CommonState.getTime() - m.timestamp);
//            System.out.println("RTT in "+this.nodeId+" is "+(messageDelayTimeFromBP.get(0)));
            seedArrivalTimeStore.add(CommonState.getTime() - m.timestamp);
            if (!messageCache.containsKey(m.id)) // Checking the received data is already present
            {
                if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
                {
                    Long firstMessage = messageCache.keySet().iterator().next();
                    messageCache.remove(firstMessage); // Removing the first/oldest message if the cache is full
                }

                messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache

                if (m.partNumber != -1 && m.rowOrColumnNumber == -1 && distributionStrategy == 2) // This was sent using the sharding method
                {
                    custodyData2.add((byte[][][]) m.body);
//                    messageCache.put(m.id,m);
                    // sending the metadata to all the nodes in this topic
//                    System.out.println(Arrays.deepToString((Object[]) m.body));
//                    System.out.println(messageCache.size()+" "+custodyData2.size()+" node "+this.nodeId+" "+CommonState.getTime());
                    Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,m.ackId); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
                    getRemaingRowColPart(m, myPid);
//                    gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,this.nodeId);
                }
                else if (m.partNumber == -1 && m.rowOrColumnNumber != -1 && distributionStrategy == 1) // This was sent using the normal method
                {
                    custodyData1.add((byte[][]) m.body);
                    // sending the metadata to all the nodes in this topic
                    Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,m.ackId); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
                    gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,this.nodeId);
                    startSampling();
                }
                else if(m.partNumber == -1 && m.rowOrColumnNumber != -1 && distributionStrategy==3) // This was sent using the normal method
                {
                    custodyData2.add((byte[][][]) m.body);
                    // sending the metadata to all the nodes in this topic
                    Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber,m.partNumber,m.timestamp,m.ackId); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
                    String recievedMsg = Arrays.deepToString((byte[][][]) m.body);
//                    System.out.println("Recievde from BP "+recievedMsg+" to node "+this.nodeId);
                    gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,this.nodeId);
//                    startSampling();
                }
//                System.out.println("NOde "+this.nodeId+" recieved data from:bp at "+CommonState.getTime());
            }
        }
    }

    // This function handles the data received
    public void handleData(Message m, int myPid) {
        if(distributionStrategy==1)
        {
            if (messageCache.containsKey(m.id)) // Checking if the m.id is already present;Like have we made the request for. Because the request and response msg have the same id
            {
                messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache
                return;
            }
        }
//         System.out.println("Recieved data from nodeID: "+ m.src +" to "+this.getNodeId()+" ==="+ m.dest+" at time "+CommonState.getTime());
        if (messageCache.containsKey(m.id)) // Checking if the m.id is already present;Like have we made the request for. Because the request and response msg have the same id
        {

//            System.out.println("Node ID: "+this.nodeId+" recieved the data from BP at "+CommonState.getTime());
            BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId;
            if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
            {
                for (Map.Entry<Long, Message> msg : messageCache.entrySet()) {
                    if (msg.getValue().src.equals(blockProposerId)) // Checking if the message is not sent by the block proposer
                    {
                        continue;
                    }
                    Long firstMessage = messageCache.keySet().iterator().next();
                    messageCache.remove(msg.getKey()); // Removing the first/oldest message which is not sent by the block proposer if the cache is full which
                    break;
                }
            }
        }
        messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache
        if (sentSeedingPartMsg.containsKey(m.id)) {
            seedPartArrivalTimeFromPeer.add(CommonState.getTime());
            seedPartDelayTimeFromPeer.add(CommonState.getTime()-m.timestamp);
            seedPartArrivalTimeStore.add(CommonState.getTime()-m.timestamp);
            NoOfSeedPartsRecieved++;
//            System.out.println("Hurryy!!!! recieved the seeding part");
            sentSeedingPartMsg.remove(m.id);
            custodyData2.add((byte[][][]) m.body);
            partRequestCounter--;
        }

        if (distributionStrategy==2 && partRequestCounter == 0) {
//            System.out.println("Yess");
            startSampling();
        }

        //Sending the message with data to all the peers/local mesh
        Message responseWithData = this.createMessage(m.id, Message.MSG_DATA, m.dest, m.messageTopicID, m.body, m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,-1); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
//        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, m.src);

        // sending the metadata to all the nodes in this topic
        Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "", m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,-1); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
//         gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,this.nodeId);
    }

    //Function the create the Message instance
    private Message createMessage(long id, int type, BigInteger dest, String topicID, Object body, boolean isRow, int RowOrColNum, int partNum, long timeStamp,long ackid) {
        Message msg;
        if (id == -1) //Create a new message with new message Id
        {
            msg = new Message(type, isRow, RowOrColNum, partNum,ackid);
            msg.timestamp = CommonState.getTime(); // Getting the current time/ time at which the message was created
        } else {
            msg = new Message(id, type, isRow, RowOrColNum, partNum,ackid);
            msg.timestamp = timeStamp;
        }
        msg.src = this.nodeId;
        msg.dest = dest;
        msg.messageTopicID = topicID;
        msg.body = body;

        return msg;
    }

    //Randomly find the row or column you want to sample
    //random the index for that row or col
    //Subscribe to the topic to which the row or col belongs to
    // Find the node ID of the node who is holding the required sample data
    // Send the message
    //In the message data send an idx of the row/col to be sampled
    public void sampleDataRequest() {
//        System.out.println("This node is sampling " + this.nodeId);
        Random random = new Random();

        int rowOrColDecider = random.nextInt(2);
        int randomSampleIndex = random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512)); // 0 to 262143 inclusive (total numbers of rows/col = 512;each contains 512 to samples;512*512=262144)
        int SamplingIdx = randomSampleIndex; // get sample a particular idx at a given row/col
        int rowOrColNo = randomSampleIndex;
        int topicNo = rowOrColNo / ((Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC", 16) / 2));
        Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo + 1));
        if (topicToSubscribe == null) {
//                System.out.println("error");

            return;
        }
        this.subscribeTopic(topicToSubscribe); // Subscribe to the topic to which the row/col to be sampled belongs to

        int nodeTopicPosition = (8 * rowOrColDecider) + ((rowOrColNo / 64));
        int nodeIndex = 0;
        if (distributionStrategy == 1) {
            int sampleHolderNodeIdx = random.nextInt(8); // Random number generated to select which the node to send the request as 8 nodes hold the same rows/cols
            nodeIndex = ((rowOrColNo % 8)*8) + sampleHolderNodeIdx*1; // As 8 nodes hold the same rows/cols.
        }
        else if(distributionStrategy==2)
        {
            nodeIndex = random.nextInt(64); //As 64 nodes in the topic will hold 8 rows/cols as they willl reconstruct the whole row/col after reciveing the parts. So you can sample any node
        }
        else if(distributionStrategy==3)
        {
            int sampleHolderNodeIdx = random.nextInt(2); // Random number generated to select which the node to send the request as 2 nodes hold the same rows/cols
            if(rowOrColNo%2==0) // If the rowOrColNo is divible by 2 then (rowOrColNo % 8) will point to the first node out of the two nodes else the send node
            {
            nodeIndex = (rowOrColNo % 8) + sampleHolderNodeIdx*1; // As 2 nodes hold the same rows.
            }
            else
            {
                nodeIndex = (rowOrColNo % 8) - sampleHolderNodeIdx*1;
            }
        }

//            System.out.println(nodeTopicPosition);

        BigInteger destId = null;
        if (rowOrColDecider == 0) { // Row sampling
            // Nodes 0-7 handle rows
            destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(nodeIndex).getProtocol(gossipSubId))).nodeId;
        } else { // Column sampling
            // Nodes 8-15 handle columns
            destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(nodeIndex + 64).getProtocol(gossipSubId))).nodeId;
        }
        if (destId == this.nodeId) {
//                System.out.println("cant sample bcz destId is equal to current nodeId");
//                System.out.println(destId+" "+this.nodeId);
            sampleDataRequest();
            return;
        }

        Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(SamplingIdx), rowOrColDecider == 0, rowOrColNo, -1, 0,-1);//Need to change/calculate the part the partNo. Just added some random value atm

        //Currently sending the sample request to the first member in the topic as we need to make changes in the row/col distributor
//            System.out.println(destId);
//            System.out.println(SamplingIdx);
//            topicToSubscribe = CustomDistribution.topics.get("Topic-" + (1));
//            BigInteger destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(0).getProtocol(gossipSubId))).nodeId;
//
//            System.out.print("Sent sampling request to node "+destId+"row/col = "+rowOrColNo+" TOPIC NUMBER "+(topicNo+1)+" node idx "+nodeIndex+" is row ");
//            System.out.println(rowOrColDecider==0);
//            System.out.println(SamplingIdx);

//            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(SamplingIdx),rowOrColDecider==0,rowOrColNo);
//            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(0),rowOrColDecider==0,0,0);
        this.sentMsg.put(sampleReqMes.id, sampleReqMes);
        NoOfSampleRequestsSent++;
//        System.out.println("Number of sample request sent "+NoOfSampleRequestsSent);
        this.publishMessage(sampleReqMes, destId, gossipSubId);

        //Setting the timeout. This will node will send the sample request again if it doesn't recieve the sample within given time
        peersim.gossipsub.Timeout t = new peersim.gossipsub.Timeout(1, destId, sampleReqMes.id);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(sampleReqMes.dest);
        long latency = transport.getLatency(src, dest);

        EDSimulator.add(4 * latency, t, src, gossipSubId); // set delay = 2*RTT

    }

    //Repeat this 75 times
    //Make sure that the sampling is not done for the same samples---------------------------------------
    public void startSampling() {
        for (int i = 0; i < 75; i++) {
            sampleDataRequest();
        }
    }


    public void handleSampleRequest(Message m, int myPid) //Sending the requested sample data if the current node has it
    {
//        System.out.println("Request received for sample");
        boolean requestedRow = m.isRow;
        int rowOrColNumb = m.rowOrColumnNumber;
        int idx = Integer.parseInt((String) m.body);
//        System.out.println("This node recieved a sampling request "+this.nodeId);
//        System.out.println(idx);
//        System.out.println("Message cashe size "+messageCache.size()+" this node id "+this.nodeId+ " time "+CommonState.getTime());
        BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId;

        for (Map.Entry<Long, Message> msg : messageCache.entrySet()) {
            Message curMessage = msg.getValue();
            if (distributionStrategy == 1) {
//            System.out.println("*****");
//                if(curMessage.src.compareTo(blockProposerId)==0)

//            System.out.print(curMessage.isRow+" "+m.isRow+" ");
//            System.out.print(curMessage.rowOrColumnNumber+" "+m.rowOrColumnNumber+" ");
//            System.out.println(curMessage.messageTopicID+" "+m.messageTopicID);
//            System.out.println(this.nodeId);
//            System.out.println("*****");
                if (curMessage.src.equals(blockProposerId) && curMessage.isRow == m.isRow && curMessage.rowOrColumnNumber == m.rowOrColumnNumber) // Checking if the message is not sent by the block proposer
                {
//                    System.out.println("yes inside");
//                    System.out.println("nodeID "+this.nodeId);
                    byte[][] data = (byte[][]) curMessage.body;  //If the number stored at this index is greater than 127 it will store the negative value as byte is type is signed meaning the values range in 127 to -127
                    byte sampleDataResp = data[0][idx];
                    Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, m.src, m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,m.ackId);
                    this.publishMessage(sampleResponse, m.src, gossipSubId);
                    return;
                }
                else if(!custodyData1.isEmpty() && curMessage.body==custodyData1.get(0))
                {
//                    System.out.print("Received sampling request "+this.nodeId+"row/col = "+curMessage.rowOrColumnNumber+" TOPIC NUMBER "+(curMessage.messageTopicID)+" is row ");
//                    System.out.println(curMessage.isRow==true);

                }

            }}
         if (distributionStrategy == 2) {
//                System.out.println("yesssss");
//                if (curMessage.partNumber != -1 && curMessage.rowOrColumnNumber == -1 && curMessage.rowOrColumnNumber == m.rowOrColumnNumber && curMessage.isRow == m.isRow) {

                    if (custodyData2.size() == 4) // checking if the node recieved half the row/cow (4 out of 8 parts)
                    {
                        byte[][][] data = (byte[][][]) custodyData2.get(0);//Currently we are just sending random sample response as we are not forming the whole row/col over here but in reality the will form the whole row/col if it has 50% cells
                        byte sampleDataResp = data[0][0][0];
                        Message sampleResponse = createMessage(m.id, Message.MSG_SAMPLE_DATA_RESPONSE, m.src, m.messageTopicID, Integer.toString(sampleDataResp), m.isRow, m.rowOrColumnNumber, m.partNumber, m.timestamp,m.ackId);
                        this.publishMessage(sampleResponse, m.src, gossipSubId);
                    }
                    else
                    {
//                        System.out.println(this.nodeId+ " No!!!!!!");
                    }
//                }

            }
//        }

    }

    public void handleSampleResponse(Message m, int myPid) {

        sampleArrivalTime.add(CommonState.getTime());
        sampleDelayTime.add(CommonState.getTime() - m.timestamp);
        samplingRTTTimeStore.add(CommonState.getTime());
        NoOfSamplesRecieved++;
        if (m.body != null) {
//            System.out.println("Yahoooo! got the sample data from node "+m.src);
//            System.out.println(m.body);
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
                // boolean b = Objects.equals(this.nodeId, m.dest);

                if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId) {
//                    System.out.println("Received  data from " + m.src + " to " + this.nodeId + " For topic "+ m.messageTopicID + " " + i++ + " " + m.id);
//                    System.out.println();
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
                if(distributionStrategy==1)
                {
                    blockProducer();
                }
                else if(distributionStrategy==2)
                {
                   shardingBasedDistribution();
                }
                else{
                redencdancyBasedDistribution();
                }
                break;
            case Message.MSG_START_SAMPLING:
                m = (Message) event;
                startSampling();
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
//                    System.out.println("Already removed");
                }
                break;


            case peersim.gossipsub.Timeout.TIMEOUT: // timeout
                peersim.gossipsub.Timeout t = (Timeout) event;
                if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
                    this.NoOfSampleRequestsSent++;
//                System.out.println("Sample request message TimeOut. Sample not recieved. Sent a sample request again");
                    sampleRequestUnsuccessful++;
//                System.out.println("sample request unsuccessfull "+ sampleRequestUnsuccessful);
                    //Send sample request again
                    Message sampleMsgSent = sentMsg.get(t.msgID);
                    sentMsg.remove(t.msgID); // remove form sentMsg
                    Message msgToResend = this.createMessage(-1, sampleMsgSent.type, sampleMsgSent.dest, sampleMsgSent.messageTopicID, sampleMsgSent.body, sampleMsgSent.isRow, sampleMsgSent.rowOrColumnNumber, sampleMsgSent.partNumber, 0,sampleMsgSent.ackId);
                    sentMsg.put(msgToResend.id, msgToResend);
//                    System.out.println("Sent message again to "+msgToResend.dest);
                    publishMessage(msgToResend, msgToResend.dest, myPid);

                } else if (sentSeedingPartMsg.containsKey(t.msgID)) {

                    Message sentMsg = sentSeedingPartMsg.get(t.msgID);
                    sentSeedingPartMsg.remove(t.msgID);
                    Message msgToResend = this.createMessage(-1, sentMsg.type, sentMsg.dest, sentMsg.messageTopicID, sentMsg.body, sentMsg.isRow, sentMsg.rowOrColumnNumber, sentMsg.partNumber, 0,sentMsg.ackId);
                    sentSeedingPartMsg.put(msgToResend.id, msgToResend);
//                    System.out.println("Sending part request to node "+msgToResend.dest+ " for part "+msgToResend.partNumber+" row/col "+msgToResend.rowOrColumnNumber + " by node "+this.nodeId+msgToResend.isRow);
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

//Trying to implement
//The validator can recontruct the row/columns if it has 50%(256 cells) of each row/col
//The BP does send the whole row/col divided into parts, where number of parts equals to number of rows/cols in a topic.
//So the first 8 nodes in the topic will hold the row parts and the next 8 nodes will hold the column parts
//So the first node will fold the 1st parts of all the 8 rows which are divided into 8 parts

    private void blockProducer() {
//        System.out.println(Configuration.getInt("NUMBER_COPIES_DISTRIBUTED"));
        int rowNumber = 0;
        int columnNumber = 0;

        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS")); // Creating a block

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId)); // Get the protocol instance of the block proposer

        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            int cnt = 0;
            int nodeCounter =0;
//             System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {
                if (cnt < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2) // Allocating first 8 nodes in the topic with rows if NUMBER_OF_ROWSCOLS_IN_A_TOPIC=16
                {
                    byte[][] rowToSend = b.getRowData(rowNumber); // row to be sent

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
//                    System.out.println("Row number is "+rowNumber+" node ID "+destID+" for topic "+topicEntry.getValue().topicID);
                    Message newMessage = new Message(3, rowToSend, true, rowNumber, -1,-1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                    newMessage.dest = destID;

//                     System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to validator node
                    nodeCounter++;
                    if(nodeCounter%8==0){
                    rowNumber++;
                    cnt++;
                    }
                } else // Allocating the rest 8 nodes in the topic with columns
                {
                    if (columnNumber == 512) // added this as it causing indexoverflow error as there is a bug in row/col allocation
                    {
                        continue;
                    }
                    byte[][] colToSend = b.getColumnData(columnNumber);

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
//                    System.out.println("Col number is "+columnNumber+" node ID "+destID+" for topic "+topicEntry.getValue().topicID);
                    Message newMessage = new Message(3, colToSend, false, columnNumber, -1,-1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                    newMessage.dest = destID;
//                     System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId);
                    // System.out.println("NodeId for given topic is:"+destID);
//                    System.out.println("Node" + destID+" has column "+columnNumber);
                    nodeCounter++;
                    if(nodeCounter%8==0) {
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
        int numberOfDivisions = Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2;// Equals to the number of rows/columns per topic
        int partSize = Configuration.getInt("NUMBER_OF_ROWS") / numberOfDivisions;

        Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS")); // Creating a block
        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId)); // Get the protocol instance of the block proposer
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
            int nodeCounter =0;

//             System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {

                if (cnt < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2) // Allocating first 8 nodes in the topic with rows if NUMBER_OF_ROWSCOLS_IN_A_TOPIC=16
                {
                    byte[][][] rowsToSend = new byte[8][][];
                    int start = rowPartNo * partSize;
                    int end = start + partSize;
//                    System.out.println("*******");
                    for (int i = 0; i < numberOfDivisions; i++) {
                        rowsToSend[i] = Arrays.copyOfRange(b.getRowData(curTopicNumber * 8 + i), start, end);
                    }
//                    System.out.println(Arrays.toString(b.getRowData(curTopicNumber * 8 + 1)));
                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    Message newMessage = new Message(3, rowsToSend, true, -1, rowPartNo,-1);
//                    System.out.println("Row parts-"+rowPart+" sent to "+destID+ " from BP "+" "+Arrays.deepToString(rowsToSend));
//                    System.out.println("*****");
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    newMessage.dest = destID;

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to validator node

                    nodeCounter++;
                    if(nodeCounter%8==0){
                    rowPartNo++;
                    cnt++;
                    }
                } else // Allocating the rest 8 nodes in the topic with columns
                {
//                    if (columnNumber == 512) // added this as it causing indexoverflow error as there is a bug in row/col allocation
//                    {
//                        continue;
//                    }
                    byte[][][] colsToSend = new byte[8][][];
                    int start = colPartNo * partSize;
                    int end = start + partSize;
                    for (int i = 0; i < numberOfDivisions; i++) {
                        colsToSend[i] = Arrays.copyOfRange(b.getColumnData(curTopicNumber * 8 + i), start, end);
                    }

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    Message newMessage = new Message(3, colsToSend, false, -1, colPartNo,-1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    newMessage.dest = destID;

                    this.publishMessage(newMessage, destID, gossipSubId);

                    nodeCounter++;
                    if(nodeCounter%8==0){
                    cnt++;
                    colPartNo++;
                    }
                }
            }
            curTopicNumber++;
        }

        System.out.println("*********Block proposer has sent the messages ********");
    }

    private void redencdancyBasedDistribution()
    {
//        System.out.println(Configuration.getInt("NUMBER_COPIES_DISTRIBUTED"));
        int rowNumber = 0;
        int columnNumber = 0;

        Block b = new Block(Configuration.getInt( "NUMBER_OF_ROWS"), Configuration.getInt(  "NUMBER_OF_COLUMNS")); // Creating a block

        peersim.gossipsub.GossipSubProtocol iGossipBlockProposer = (peersim.gossipsub.GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId)); // Get the protocol instance of the block proposer

        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());

        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            int cnt = 0;
//             System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {
                if (cnt < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC")/2) // Allocating first 8 nodes in the topic with rows if NUMBER_OF_ROWSCOLS_IN_A_TOPIC=16
                {
                    byte[][][] rowsToSend = new byte[2][][];
                    rowsToSend[0] = Arrays.copyOfRange(b.getRowData( rowNumber), 0, Configuration.getInt( "NUMBER_OF_ROWS")/2); // row to be sent
                    rowsToSend[1] = Arrays.copyOfRange(b.getRowData( rowNumber+1), 0, Configuration.getInt( "NUMBER_OF_ROWS")/2);
                    BigInteger destID = ((peersim.gossipsub.GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
//                    System.out.println("Row number is "+rowNumber);
                    Message newMessage = new Message(3, rowsToSend,true,rowNumber,-1,-1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                    newMessage.dest = destID;
//                    System.out.println("BP sent "+Arrays.deepToString(rowsToSend)+" to node "+ destID);
//                     System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to validator node

                    cnt++;
                    if(cnt%2==0)
                    {
                        rowNumber+=2;
                    }
                } else // Allocating the rest 8 nodes in the topic with columns
                {
                    if (columnNumber == 512) // added this as it causing indexoverflow error as there is a bug in row/col allocation
                    {
                        continue;
                    }
                    byte[][][] colsToSend = new byte[2][][];
                    colsToSend[0] = Arrays.copyOfRange(b.getColumnData( columnNumber), 0, Configuration.getInt( "NUMBER_OF_ROWS")/2); // row to be sent
                    colsToSend[1] = Arrays.copyOfRange(b.getColumnData( columnNumber+1), 0, Configuration.getInt( "NUMBER_OF_ROWS")/2);

                    BigInteger destID = ((peersim.gossipsub.GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                    Message newMessage = new Message(3,colsToSend ,false,columnNumber,-1,-1);
                    newMessage.src = this.getNodeId();
                    newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                    newMessage.dest = destID;
//                     System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId);
                    // System.out.println("NodeId for given topic is:"+destID);
//                    System.out.println("Node" + destID+" has column "+columnNumber);
                    cnt++;
                    if(cnt%2==0)
                    {
                        columnNumber+=2;
                    }

                }
            }
        }
        System.out.println(rowNumber);
        System.out.println(columnNumber);
        System.out.println("*********Block proposer has sent the messages ********");
    }


}



//Trying to implement
//The validator can recontruct the row/columns if it has 50%(256 cells) of each row/col
//The BP doesn't send the whole row/col
//The BP sends the half of the whole row/col which is divided into 2 parts

// Compile command; Open terminal in src dir.
// javac -cp "../lib/peersim-1.0.5.jar;../lib/other-dependency.jar" -d
// ../classes peersim\GossipSub\*.java
// Run the stimulator; open terminal in PROJECT directory
// java -cp "classes;lib\peersim-1.0.5.jar;lib\jep-2.3.0.jar;lib\djep-1.0.0.jar"
// peersim.Simulator Config1.cfg

// Add the body to all the messages -----------------------------------------
// Do all the nodes in the mesh send IHave message for the same messgae/Date?-------
// How? what is added in the queue?How big is it? QUESTION----------

// For sampling, you can start after you receives the data from the block producer
// To determine who holds which row or column, you can a global state; loop over each Validator node and find what they hold using the RowColumnDistributor
// To implement bandwidth, keep a queue for indivual gossipsub protocol instace storing the message they want to send
// Keep a time variable, waitUntilMessageSent in Long  data type, which stores the when the sent message will reach, so to wait until the previous message is completely sent before sending the next on. Add this waiting time in latency

// Note we are assuming
//1.There are no mallicious nodes
//2. The transport protocol is reliable(No data is lost)
