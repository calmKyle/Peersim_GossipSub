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

public class GossipSubProtocol implements Cloneable, EDProtocol {

    private static final String PAR_TRANSPORT = "transport";
    private static String prefix = null;
    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;

    /**
     * nodeId of this pastry node
     */
    public BigInteger nodeId;

    protected int degree;
    private int degreeLow;
    private int degreeHigh;
    private int i = 0;

    // The message cache stores recent messages Only add messages with data All the messages(IHAVE,IWANT,DATA) have the same message ID for the same data, so it is easy to compare/store in messageCache
    private LinkedHashMap<Long, Message> messageCache; // <MessageID,Message>

    // Maximum size of messageCashe
    public static final int MESSAGE_CACHE_SIZE = 1024; // Just took a random number atm. May need to update it--------------------------------

    private float heartbeatInitialDelay;
    private int heartbeatInterval;

    // Stores the topic ids of the which it is associated with
    private Set<Topic> subscribedTopics;

    // Maps each topic the current node is a member to a set of peer IDs that are part of its local mesh.
    // This represents the direct connections in the local mesh.
    protected Map<String, Set<BigInteger>> localMesh;

    // List all the topic for which this node is a member. Mapping of topics to list of all the members in a given topic
    protected Map<String, Set<BigInteger>> topicNodes; // <Topic Name, Arraylist<All the members in this topic>>

    private ArrayList<byte []> custodyData; //This arraylist contains the rows/cols it was intially given by the block proposer to custody
    //Logic 1: Right now the node will forward the initial data to all the peers in the that topic meaning that eventually all the peers in that topic will hold that data as well.
    //Logic 2: To avoid to forward the initial data received from the data proposer to the nodes in the topic we can data that data to the custodyData arraylist and not to the message cashe and hence not forward that message to our peers. Just send the message with the metadata.

//    private int maximumBandwidth;
//    private int currentBandwidth;
    private int interfaceBandwidth;
    private int blockProducerBandwidth;
//    private long waitUntilMessageSent;
    private long lastMessageTransmissionTime; // The transmission time of the new/latest message added to the queue(transmission time for the last element in the messageQueue)

    private ArrayList<Message>messageQueue = new ArrayList<>(); // arraylist to store the messages to be sent from this node
    private ArrayList<Long>messageTransmissionDelayQueue = new ArrayList<>(); // arraylist to transmission delay time for the message to be sent

    //Stats for messages delivered from the block proposer(BP)
    public IncrementalStats seedArrivalTimeStore = new IncrementalStats(); // The time at which the seeds(rows or columns) arrived from the block proposer
    public ArrayList<Long> messageArrivalTimeFromBP; // It stores all the time stamp at which the message with the data was arrivied from the block proposer
    public ArrayList<Long> messageDelayTimeFromBP; // It stores all the difference bt. the message arrival time(the time at which the message was received) and the time the message was created/sent from the block proposer. So in short it gives the time it took for the message to reach the destination

    //Stats for sampling messages successfully recieved after sending the request
    public IncrementalStats samplingRTTTimeStore = new IncrementalStats();
    public ArrayList<Long> sampleArrivalTime; // It stores all the time stamp at which the message with the data was arrivied after the current node sent sample request
    public ArrayList<Long> sampleDelayTime; // It stores all the difference bt. the sample arrival time(the time at which the sample was received) and the time the message(sample request was sent) was created/sent from the block proposer. So it the net time from this node sending the sample the request, the sample holder recieving and sending the sample and the sample arriving to the current node
    public int sampleRequestUnsuccessful; //It stores the number of sample request that timed out and had to send the sample request again
    public int NoOfSampleRequestsSent; // It stores the total number of sample requests sent

//    trace the sample request message sent for timeout purpose
    private TreeMap<Long, Long> sentMsg;

    public Object clone()
    {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        return cln;
    }

    public GossipSubProtocol(String prefix)
    {
        this.degree = 8;
        this.degreeLow = 6;
        this.degreeHigh = 14;

        this.localMesh = new HashMap<>();

        this.messageCache = new LinkedHashMap<>();

        this.heartbeatInitialDelay = 0.0f;
        this.heartbeatInterval = 0;

        this.subscribedTopics = new HashSet<>();

        this.topicNodes = new HashMap<>();

        this.nodeId = null;

        this.custodyData = new ArrayList<>();

//        this.maximumBandwidth = Configuration.getInt("MAX_BANDWIDTH", 2200);
//        this.currentBandwidth = 0;
        this.interfaceBandwidth = Configuration.getInt("INTERFACE_BANDWIDTH", 100000000);
        this.blockProducerBandwidth = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1000000000);

        GossipSubProtocol.prefix = prefix;

        tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

        this.messageArrivalTimeFromBP = new ArrayList<>();
        this.messageDelayTimeFromBP = new ArrayList<>();

        this.sampleArrivalTime = new ArrayList<>();
        this.sampleDelayTime = new ArrayList<>();

        this.sampleRequestUnsuccessful=0;
        this.NoOfSampleRequestsSent=0;

        sentMsg = new TreeMap<Long, Long>();
    }

    // Calculating the size of the message in bytes
    private int calculateMessageSize(Message message) {
        int size = 0;
        size+=Integer.BYTES; //Message Type
        size += Long.BYTES; // Message ID
        size += message.src.toString().getBytes().length; // Source ID size
        size += message.dest.toString().getBytes().length; // Destination ID size
        if(message.messageTopicID!=null) {
            size += message.messageTopicID.getBytes().length; // Topic ID size
        }
        if(message.body!=null){
        if (message.body instanceof byte[]) {
            size += ((byte[]) message.body).length; // Size of the body (byte array)
        } else if (message.body instanceof String) {
            size += ((String) message.body).getBytes().length; // Size of the body (String)
        }
        }
        size += 1; // For isRow flag
        size += Integer.BYTES; // For rowOrColumnNumber
        size+=Long.BYTES; // For the timestamp
//        System.out.println("size "+size);
        return size;
    }

    // Topic subscription function. Used to subscribe to a particular topic
    public void subscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic))
        {
            return;
        }
        subscribedTopics.add(topic);
        // System.out.println("Node with ID: "+this.getNodeId()+" Subscribed to topic: "+topic.topicID);
        localMesh.put(topic.topicID, new HashSet<>());
    }

    // Topic unsubscription function
    public void unsubscribeTopic(Topic topic)
    {
        if (subscribedTopics.contains(topic))
        {
            subscribedTopics.remove(topic);
            localMesh.remove(topic.topicID);
            return;
        }
    }

    //Function to add a topic to which this node is subsribed and its member
    public void setTopicMembersList(String topicName, Set<BigInteger> members)
    {
        topicNodes.put(topicName, members);
    }

    //Function to send the message in the network
    public void publishMessage(Message m, BigInteger destId, int myPid) {
        int bandwidth = 0;
        if (this.nodeId == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId)
        {
            bandwidth=blockProducerBandwidth;
        }
        else
        {
            bandwidth = interfaceBandwidth;
        }
        this.messageCache.put(m.id, m);

        long currentTime = 0;
        long queuingDelay = 0;

        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);

        long latency = transport.getLatency(src, dest); // Calculate message delay/propogation delay

        if(messageQueue.isEmpty())
        {
            queuingDelay = 0;
        }
        else
        {
            while(!messageTransmissionDelayQueue.isEmpty() && CommonState.getTime()>messageTransmissionDelayQueue.get(0))
            {
                messageQueue.remove(0);
                messageTransmissionDelayQueue.remove(0);
            }
            if(!messageQueue.isEmpty()) {
//                System.out.println("Delay!!!!");
                queuingDelay = messageTransmissionDelayQueue.get(messageTransmissionDelayQueue.size() - 1) - CommonState.getTime();
            }else
            {
                queuingDelay = 0;
            }
        }
        int messageSize = calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / (double)bandwidth); // Use Math.ceil to round up the transmission time so that even fractional transmission time are rounded up to the next whole unit for accurate message transmission.
        long propogationDelay = latency; // Do we need add propogation delay when sceduling the event??
        long totalDelay = queuingDelay+transmissionDelay+propogationDelay;
        if(totalDelay!=0){
//        System.out.println("Delay at src node "+this.nodeId+ "to reciving node "+ m.dest+ " Queuing delay "+queuingDelay+" progation delay "+propogationDelay+" transmission delay "+transmissionDelay+" sceduled at "+CommonState.getTime()+totalDelay);
        }
        EDSimulator.add(totalDelay, m, dest, myPid);
        long waitUntilMessageSent = CommonState.getTime() + transmissionDelay;
        messageQueue.add(m);
        messageTransmissionDelayQueue.add(waitUntilMessageSent);
        lastMessageTransmissionTime = waitUntilMessageSent;

//        currentBandwidth += msgSize;
        // System.out.println("Sending the message to nodeID-1:"+destId);


//         System.out.println("Sending the message to nodeID-2:"+dest.getID()+" "+msgSize+" "+m.id);
        // System.out.println(Objects.equals(this.nodeId, m.src));
//        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
//        transport.send(src, dest, m, gossipSubId); //========THIS LINE IS CAUSING THE ERROR========

        // System.out.println("Transport protocol class: " +
        // Network.prototype.getProtocol(tid).getClass().getName());

        // Schedule the delivery of the message to the destination node
//        EDSimulator.add(latency, m, dest, myPid);
        // May add timeout as in kademlia----------------------------------

        //Use transport protocol to send the message instead of EDSimulator. as transport protocol eventually calls EDSimulator. Just use EDSimulator in the initializer where the block proposer sends the rows/cols to the validator nodes
    }

    // Send Message to all nodes in topic
    public void sendMessageToTopicNodes(Message m, int myPid, String topicID, Map<String, Set<BigInteger>> nodesInTopic,BigInteger messageSender) {
        Set<BigInteger> topicNodes = nodesInTopic.get(topicID);

        for (BigInteger peerId : topicNodes) {
            if (peerId == messageSender) // Condition to avoiding sending the message back to the node who forwarded the message to us
            {
                // System.out.println("Dont send the message as we recieved this message from it");
                continue;
            }

            Message newMessage = m.copy(); // Getting the cloned message instance
            newMessage.dest = peerId;
            publishMessage(newMessage, peerId, myPid);
        }
    }

    // Send message to all the peers/local mesh in the given topic
    public void sendMessageToPeers(Message m, int myPid, String topicID, BigInteger avoid)
    {
         if (this.localMesh.get(topicID).isEmpty())
         {
             System.out.println("The local mesh for node: "+this.nodeId+ " is empty");
             return;
             // System.out.println("Called sendMessageToPeers in " + this.nodeId);
         }
        sendMessageToTopicNodes(m, myPid, topicID, this.localMesh, avoid);
    }

    // Send the Metadata all the nodes/global mesh in the topic
    // Implement that a random no of nodes are selected for gossiping-------------------
    public void gossipMessageToTopicNodes(Message m, int myPid, String topicID, BigInteger avd)
    {
        sendMessageToTopicNodes(m, myPid, topicID, this.topicNodes, avd);
    }

    // Function to check if the message has been seen before
    public boolean isMessageSeen(Message m)
    {
        if (messageCache.containsKey(m.id))
        {
            return true;
        }
        return false;
    }

    //Function to handle the recieved IHave messages
    public void handleIHave(Message m, int myPid)
    {
        if (isMessageSeen(m))// Message already present in the cache / already seen
        {
            return;
        }
        else // Do we need to do this---------- (Might implememnt a randomly; whether to make a request for the data or not)-------
        {
            // Create IWANT Request Message to request the data
            // Keeping the id of the new message same as the IHave message
            Message request = this.createMessage(m.id, Message.MSG_IWANT, m.src, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber,m.timestamp);

            publishMessage(request, m.src, myPid);
        }
    }

    // Send data to the node who requested/sent IWant message
    // Compare the Iwant messages with the requested data message and send that
    public void handleIWANT(Message m, int myPid) 
    {
        if (messageCache.containsKey(m.id)) 
        {
            // Create DATA/response Message
            Message response = this.createMessage(m.id, Message.MSG_DATA, m.src, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber,m.timestamp);

            // send data/response to IWANT Request
            publishMessage(response, m.src, myPid);
        }
    }

    public void handleBlockProducerData(Message m, int myPid) {
//        System.out.println("Received data from block producer");
//        System.out.println("Recieved data from nodeID: "+ m.src +" to "+this.getNodeId()+" ==="+ m.dest+" at time "+CommonState.getTime());
        if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId)
        {
//            System.out.println("Node ID: "+this.nodeId+" recieved the data from BP at "+CommonState.getTime());

            messageArrivalTimeFromBP.add(CommonState.getTime());
//            System.out.println("Time in "+this.nodeId+" protocol is "+CommonState.getTime());
            messageDelayTimeFromBP.add(CommonState.getTime()-m.timestamp);
//            System.out.println("RTT in "+this.nodeId+" is "+(messageDelayTimeFromBP.get(0)));
            seedArrivalTimeStore.add(CommonState.getTime()-m.timestamp);
            if (!messageCache.containsKey(m.id)) // Checking the received data is already present
            {
                this.seedArrivalTimeStore.add(CommonState.getTime());
                if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
                {
                    Long firstMessage = messageCache.keySet().iterator().next();
                    messageCache.remove(firstMessage); // Removing the first/oldest message if the cache is full
                }

                messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache
                custodyData.add((byte[]) m.body);
//                System.out.println("NOde "+this.nodeId+" recieved data from:bp at "+CommonState.getTime());

                // sending the metadata to all the nodes in this topic
                Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber,m.timestamp); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
//                 gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,this.nodeId);
            }
        }
        startSampling();
    }

    // This function handles the data received
    public void handleData(Message m, int myPid)
    {
//         System.out.println("Recieved data from nodeID: "+ m.src +" to "+this.getNodeId()+" ==="+ m.dest+" at time "+CommonState.getTime());

        if (!messageCache.containsKey(m.id)) // Checking the received data is already present
        {
            System.out.println("Node ID: "+this.nodeId+" recieved the data from BP at "+CommonState.getTime());
            BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId;
            if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
            {
                for (Map.Entry<Long, Message> msg : messageCache.entrySet())
                {
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

        //Sending the message with data to all the peers/local mesh
        Message responseWithData = this.createMessage(m.id, Message.MSG_DATA, m.dest, m.messageTopicID, m.body,m.isRow,m.rowOrColumnNumber,m.timestamp); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, m.src);

        // sending the metadata to all the nodes in this topic
        Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber,m.timestamp); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
//         gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID,this.nodeId);
    }
    //Function the create the Message instance
    private Message createMessage(long id, int type, BigInteger dest, String topicID, Object body, boolean isRow, int RowOrColNum,long timeStamp)
    {
        Message msg;
        if(id==-1) //Create a new message with new message Id
        {
            msg = new Message(type,isRow,RowOrColNum);
            msg.timestamp = CommonState.getTime(); // Getting the current time/ time at which the message was created
        }
        else
        {
            msg = new Message(id, type,isRow,RowOrColNum);
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
    public void sampleDataRequest()
    {
//            System.out.println("This node is sampling "+this.nodeId);
            Random random = new Random();

            int rowOrColDecider = random.nextInt(2);
            int randomSampleIndex = random.nextInt(Configuration.getInt("NUMBER_OF_COLUMNS", 512)); // 0 to 262143 inclusive (total numbers of rows/col = 512;each contains 512 to samples;512*512=262144)
            int SamplingIdx = randomSampleIndex; // get sample a particular idx at a given row/col
            int rowOrColNo = randomSampleIndex;
            int topicNo = rowOrColNo / ((Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC", 16)/2));
            Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo+1));
            if (topicToSubscribe == null) {
//                System.out.println("error");
//                System.out.println(topicNo);
//                System.out.println(rowOrColNo);
//                System.out.println(randomSampleIndex);
//                System.out.println(SamplingIdx);
//                System.out.println(topicNo);
                return;
            }
            this.subscribeTopic(topicToSubscribe); // Subscribe to the topic to which the row/col to be sampled belongs to

            int nodeTopicPosition = (8*rowOrColDecider)+((rowOrColNo/64));
            int nodeIndex = rowOrColNo % 8;
//            System.out.println(nodeTopicPosition);

            BigInteger destId = null;
            if (rowOrColDecider == 0) { // Row sampling
                // Nodes 0-7 handle rows
                destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(nodeIndex).getProtocol(gossipSubId))).nodeId;
            } else { // Column sampling
                // Nodes 8-15 handle columns
                destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(nodeIndex + 8).getProtocol(gossipSubId))).nodeId;
            }
            if(destId==this.nodeId)
            {
//                System.out.println("cant sample bcz destId is equal to current nodeId");
//                System.out.println(destId+" "+this.nodeId);
                sampleDataRequest();
                return;
            }

            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(SamplingIdx),rowOrColDecider==0,rowOrColNo,0);

            //Currently sending the sample request to the first member in the topic as we need to make changes in the row/col distributor
//            System.out.println(destId);
//            System.out.println(SamplingIdx);
//            topicToSubscribe = CustomDistribution.topics.get("Topic-" + (1));
//            BigInteger destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(0).getProtocol(gossipSubId))).nodeId;
//
//            System.out.println(destId);
//            System.out.println(SamplingIdx);

//            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(SamplingIdx),rowOrColDecider==0,rowOrColNo);
//            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(0),rowOrColDecider==0,0,0);
        this.sentMsg.put(sampleReqMes.id, sampleReqMes.timestamp);
        NoOfSampleRequestsSent++;
//        System.out.println("Number of sample request sent "+NoOfSampleRequestsSent);
            this.publishMessage(sampleReqMes,destId,gossipSubId);

            //Setting the timeout. This will node will send the sample request again if it doesn't recieve the sample within given time
            peersim.gossipsub.Timeout t = new peersim.gossipsub.Timeout(destId, sampleReqMes.id);
            Node src = CustomDistribution.networkNodes.get(this.nodeId);
            Node dest = CustomDistribution.networkNodes.get(sampleReqMes.dest);
            long latency = transport.getLatency(src, dest);

            EDSimulator.add(4 * latency, t, src, gossipSubId); // set delay = 2*RTT

    }

    //Repeat this 75 times
    //Make sure that the sampling is not done for the same samples---------------------------------------
    public void startSampling()
    {
        for(int i=0;i<75;i++)
        {
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
        for (Map.Entry<Long, Message> msg : messageCache.entrySet())
        {
            Message curMessage = msg.getValue();
//            System.out.println("*****");
//            System.out.print(curMessage.src+" "+(blockProposerId)+" ");
//            System.out.print(curMessage.isRow+" "+m.isRow+" ");
//            System.out.print(curMessage.rowOrColumnNumber+" "+m.rowOrColumnNumber+" ");
//            System.out.println(curMessage.messageTopicID+" "+m.messageTopicID);
//            System.out.println(this.nodeId);
//            System.out.println("*****");
            if (curMessage.src.equals(blockProposerId) && curMessage.isRow==m.isRow && curMessage.rowOrColumnNumber==m.rowOrColumnNumber) // Checking if the message is not sent by the block proposer
            {
//                System.out.println("yes inside");
                byte[] data = (byte[]) curMessage.body;  //If the number stored at this index is greater than 127 it will store the negative value as byte is type is signed meaning the values range in 127 to -127
                byte sampleDataResp = data[idx];
                Message sampleResponse = createMessage(m.id,Message.MSG_SAMPLE_DATA_RESPONSE,m.src,m.messageTopicID,Integer.toString(sampleDataResp) ,m.isRow,m.rowOrColumnNumber,m.timestamp);
                this.publishMessage(sampleResponse,m.src,gossipSubId);
                break;
            }
        }

    }

    public void handleSampleResponse(Message m,int myPid)
    {

        sampleArrivalTime.add(CommonState.getTime());
        sampleDelayTime.add(CommonState.getTime()-m.timestamp);
        samplingRTTTimeStore.add(CommonState.getTime()-m.timestamp);
        if(m.body!=null)
        {
//            System.out.println("Yahoooo! got the sample data from node "+m.src);
//            System.out.println(m.body);
        }
    }

    @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;
        Message m;

        switch (((SimpleEvent) event).getType()) 
        {
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

                if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId)
                {
//                    System.out.println("Received  data from " + m.src + " to " + this.nodeId + " For topic "+ m.messageTopicID + " " + i++ + " " + m.id);
//                    System.out.println();
                    handleBlockProducerData(m,myPid);
                    break;
                }
                // System.out.println(this.nodeId);
                // System.out.println();
                // System.out.println();
                handleData(m, myPid);
                break;

            case Message.MSG_BLOCK_PROPOSER:
                 System.out.println("I am the block producer");
                m = (Message) event;
                System.out.println("Message sent from trafficGenerator at: "+m.timestamp);
                System.out.println("Block producer started at: "+CommonState.getTime());
//                this.maximumBandwidth = Integer.MAX_VALUE;
                blockProducer();
                break;

//            case Message.MSG_START_SAMPLING:
//                m = (Message) event;
//                startSampling();
//                break;

            case Message.MSG_SAMPLE_DATA_REQUEST:
                m = (Message) event;
                handleSampleRequest(m,myPid);
                break;

            case Message.MSG_SAMPLE_DATA_RESPONSE:
                m = (Message) event;
                if(sentMsg.containsKey(m.id))
                {
                sentMsg.remove(m.id);
                handleSampleResponse(m,myPid);
                }
                else
                {
//                    System.out.println("Already removed");
                }
                break;
//            case Message.MSG_RESET_BANDWIDTH:
//                processMessageQueue();
//                break;

            case peersim.gossipsub.Timeout.TIMEOUT: // timeout
                peersim.gossipsub.Timeout t = (Timeout) event;
                if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
                    // remove form sentMsg
                    sentMsg.remove(t.msgID);

//                System.out.println("Sample request message TimeOut. Sample not recieved. Sent a sample request again");
                sampleRequestUnsuccessful++;
//                System.out.println("sample request unsuccessfull "+ sampleRequestUnsuccessful);
                //Send sample request again
                sampleDataRequest();
                }
                break;
            case Message.MSG_EMPTY:
                break;
            // TO DO

        }
    }

    /**
     * set the current NodeId
     *
     * @param tmp BigInteger
     */
    public void setNodeId(BigInteger tmp) {
        this.nodeId = tmp;
    }

    public BigInteger getNodeId() 
    {
        return this.nodeId;
    }

    private void blockProducer() 
    {

//        System.out.println(Configuration.getInt("NUMBER_COPIES_DISTRIBUTED"));
        int rowNumber = 0;
        int columnNumber = 0;


        Block b = new Block(Configuration.getInt( "NUMBER_OF_ROWS"), Configuration.getInt(  "NUMBER_OF_COLUMNS")); // Creating a block

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId)); // Get the protocol instance of the block proposer
        
        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());
        // int cnt1 = 0;
        
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            int cnt = 0;
//             System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {
                // cnt1++;
                if(cnt==0)
                {
//                    System.out.println("Time at zero");
//                    System.out.println(CommonState.getTime());
                }

                if (cnt < Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC")/2) // Allocating first 8 nodes in the topic with rows if NUMBER_OF_ROWSCOLS_IN_A_TOPIC=16
                {
                    byte[] rowToSend = b.getRowData(rowNumber); // row to be sent

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
//                    System.out.println("Row number is "+rowNumber);
                     Message newMessage = new Message(3, rowToSend,true,rowNumber);
                     newMessage.src = this.getNodeId();
                     newMessage.messageTopicID = topicEntry.getValue().topicID;
                     // System.out.println("NodeId for given topic is:"+destID);
                     newMessage.dest = destID;

//                    Message newMessage = this.createMessage(-1,Message.MSG_DATA,destID, null, rowToSend);
//                     System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to validator node
                    rowNumber++;
                    cnt++;
                } else // Allocating the rest 8 nodes in the topic with columns
                {
                    if (columnNumber == 512) // added this as it causing indexoverflow error as there is a bug in row/col allocation
                    {
                        continue;
                    }
                    byte[] colToSend = b.getColumnData(columnNumber);

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                     Message newMessage = new Message(3, colToSend,false,columnNumber);
                     newMessage.src = this.getNodeId();
                     newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                     newMessage.dest = destID;
//                     System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

//                    Message newMessage = this.createMessage(-1,Message.MSG_DATA,destID, null, colToSend);

                    this.publishMessage(newMessage, destID, gossipSubId);
                    // System.out.println("NodeId for given topic is:"+destID);
//                    System.out.println("Node" + destID+" has column "+columnNumber);
                    columnNumber++;
                    cnt++;

                }
            }
        }
        // System.out.println("********Count in block producer is :" + cnt1);
        System.out.println(rowNumber);
        System.out.println(columnNumber);
         System.out.println("*********Block proposer has sent the messages ********");
    }
}

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
