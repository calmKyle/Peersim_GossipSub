package peersim.gossipsub;

import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;

import java.math.BigInteger;
import java.util.*;

//import static peersim.gossipsub.CustomDistribution.topics;

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

    // //List of peers with their topic IDS
    // private Map<BigInteger, Set<String>> peers; //Map<peerID,Set<TopicID>>
    //
    // //List of all the nodes who have one or more common topicIDs with ours
    // private Map<BigInteger, Set<String>> nodes; //<Node ID,Set<List of their
    // subscribed topic>>

    // Maps each topic the current node is a member to a set of peer IDs that are part of its local mesh.
    // This represents the direct connections in the local mesh.
    protected Map<String, Set<BigInteger>> localMesh;

    // List all the topic for which this node is a member. Mapping of topics to list of all the members in a given topic
    protected Map<String, Set<BigInteger>> topicNodes; // <Topic Name, Arraylist<All the members in this topic>>

    private ArrayList<int []> custodyData; //This arraylist contains the rows/cols it was intially given by the block proposer to custody
    //Logic 1: Right now the node will forward the initial data to all the peers in the that topic meaning that eventually all the peers in that topic will hold that data as well.
    //Logic 2: To avoid to forward the initial data received from the data proposer to the nodes in the topic we can data that data to the custodyData arraylist and not to the message cashe and hence not forward that message to our peers. Just send the message with the metadata.

    private double maximumBandwidth;
    private double currentBandwidth;
    private long waitUntilMessageSent;

    private Queue<Message>messageQueue = new LinkedList<>(); // Queue to store the messages to be sent from this node

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

        // this.peers = new HashMap<>();
        //
        // this.nodes = new HashMap<>();

        this.topicNodes = new HashMap<>();

        this.nodeId = null;

        this.custodyData = new ArrayList<>();

        GossipSubProtocol.prefix = prefix;

        tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
    }

    // Calculating the size of the message in bytes
    private int calculateMessageSize(Message message) {
        int size = 0;
        size+=Integer.BYTES; //Message Type
        size += Long.BYTES; // Message ID
        size += message.src.toString().getBytes().length; // Source ID size
        size += message.dest.toString().getBytes().length; // Destination ID size
        size += message.messageTopicID.getBytes().length; // Topic ID size
        if (message.body instanceof int[]) {
            size += ((int[]) message.body).length * Integer.BYTES; // Size of the body (int array)
        } else if (message.body instanceof String) {
            size += ((String) message.body).getBytes().length; // Size of the body (String)
        }
        size += 1; // For isRow flag
        size += Integer.BYTES; // For rowOrColumnNumber
        return size;
    }

    // Topic subscription function. Used to subscribe to a particular topic
    public void subscribeTopic(Topic topic) {
        // System.out.println("Subscribed to topic"+topic.topicID+" nodeID: "+this.nodeId);
        if (subscribedTopics.contains(topic))
        {
            return;
        }
        // System.out.println("Node with ID: "+this.getNodeId()+" Subscribed to topic: "+topic.topicID);

        subscribedTopics.add(topic);

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
    public void publishMessage(Message m, BigInteger destId, int myPid)
    {
        messageQueue.add(m);
        this.messageCache.put(m.id, m);
        Message messageToBeSentFromQueue = messageQueue.peek();

        int msgSize = calculateMessageSize(messageToBeSentFromQueue); // Calcualte the size of the message which is first in the messageQueue

        if(currentBandwidth+msgSize>maximumBandwidth) // maximum bandwidth exceeded wait until it resets.
        {
            return;
        }
        messageQueue.remove();
        currentBandwidth+=msgSize;
        // System.out.println("Sending the message to nodeID-1:"+destId);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(messageToBeSentFromQueue.dest);

        // System.out.println("Sending the message to nodeID-2:"+dest.getID());
        // System.out.println(Objects.equals(this.nodeId, m.src));
        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        transport.send(src, dest, messageToBeSentFromQueue, gossipSubId); //========THIS LINE IS CAUSING THE ERROR========

        // System.out.println("Transport protocol class: " +
        // Network.prototype.getProtocol(tid).getClass().getName());


        long latency = transport.getLatency(src, dest); // Calculate message delay

        // Schedule the delivery of the message to the destination node
//        EDSimulator.add(latency, m, dest, myPid);
        // May add timeout as in kademlia----------------------------------

        //Use transport protocol to send the message instead of EDSimulator. as transport protocol eventually calls EDSimulator. Just use EDSimulator in the initializer where the block proposer sends the rows/cols to the validator nodes
    }

    // Send Message to all nodes in topic
    public void sendMessageToTopicNodes(Message m, int myPid, String topicID, Map<String, Set<BigInteger>> nodesInTopic,BigInteger messageSender) {
        // Ask:--I have created a static HashMap in the initialiser is that fine.-------------------------
        // System.out.println("Sending the message to topic nodes hurry!!!!!");
        // for (Map.Entry<String, Set<BigInteger>> topicEntry : topicNodes.entrySet()) {
        // BigInteger peerId = peerEntry.getKey(); // Peer ID
        // Set<String> peerTopics = peerEntry.getValue(); // Topics the peer is
        // subscribed to
        //
        // if (peerTopics.contains(topicID)) {
        // m.dest = peerId;
        // sendMessage(m, peerId, myPid);
        // }
        // }
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
        else
        {
            // Create IWANT Request Message to request the data
            // Keeping the id of the new message same as the IHave message
            Message request = this.createMessage(m.id, Message.MSG_IWANT, m.src, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber);

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
            Message response = this.createMessage(m.id, Message.MSG_DATA, m.src, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber);

            // send data/response to IWANT Request
            publishMessage(response, m.src, myPid);
        }
    }

    public void handleBlockProducerData(Message m, int myPid) {
        if (m.src == ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId)
        {
            if (!messageCache.containsKey(m.id)) // Checking the received data is already present
            {
                if (messageCache.size() > MESSAGE_CACHE_SIZE)// Checking if the size of cache has exceeded the max value
                {
                    Long firstMessage = messageCache.keySet().iterator().next();
                    messageCache.remove(firstMessage); // Removing the first/oldest message if the cache is full
                }

                messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache
                custodyData.add((int[]) m.body);

                // sending the metadata to all the nodes in this topic
                Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
                // gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID);
            }
        }
    }

    // This function handles the data received
    public void handleData(Message m, int myPid)
    {
        // System.out.println("Recieved data from nodeID: "+ m.src +" to "+this.getNodeId()+" ==="+ m.dest);
        if (!messageCache.containsKey(m.id)) // Checking the received data is already present
        {
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
        Message responseWithData = this.createMessage(m.id, Message.MSG_DATA, m.dest, m.messageTopicID, m.body,m.isRow,m.rowOrColumnNumber); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
        sendMessageToPeers(responseWithData, myPid, m.messageTopicID, m.src);

        // sending the metadata to all the nodes in this topic
        Message responseWithMetaData = this.createMessage(m.id, Message.MSG_IHAVE, m.dest, m.messageTopicID, "",m.isRow,m.rowOrColumnNumber); // Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
        // gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID);
    }
    //Function the create the Message instance
    private Message createMessage(long id, int type, BigInteger dest, String topicID, Object body, boolean isRow, int RowOrColNum)
    {
        Message msg;
        if(id==-1) //Create a new message with new message Id
        {
            msg = new Message(type,isRow,RowOrColNum);
        }
        else
        {
            msg = new Message(id, type,isRow,RowOrColNum);
        }
        msg.src = this.nodeId;
        msg.dest = dest;
        msg.messageTopicID = topicID;
        msg.body = body;

        return msg;
    }

    public void sampleData()
    {
        //Randomly find the row or column you want to sample
        //random the index for that row or col
        //Subscribe to the topic to which the row or col belongs to
        // Find the node ID of the node who is holding the required sample data
        // Send the message
        //In the message data send an idx of the row/col to be sampled
        //Repeat this 75 times
        //Make sure that the sampling is not done for the same samples---------------------------------------
        for(int i=0;i<75;i++)
        {
            Random random = new Random();

            int rowOrColDecider = random.nextInt(2);
            int randomSampleIndex = random.nextInt(262144); // 0 to 262143 inclusive (total numbers of rows/col = 512;each contains 512 to samples;512*512=262144)
            int SamplingIdx = randomSampleIndex%512; // get sample a particular idx at a given row/col
            int rowOrColNo = (randomSampleIndex / 512);
            int topicNo = rowOrColNo / 8;
            Topic topicToSubscribe = CustomDistribution.topics.get("Topic-" + (topicNo+1));
            if (topicToSubscribe != null) {
//                System.out.println("Hurry! subscribed to the topic while sampling");
//                System.out.println(this.nodeId);
//                System.out.println(topicNo);
//                System.out.println(rowOrColNo);
//                System.out.println(randomSampleIndex);
//                System.out.println(SamplingIdx);
//                System.out.println(topicNo);
            }
            else
            {
                System.out.println("error");
                System.out.println(topicNo);
                System.out.println(rowOrColNo);
                System.out.println(randomSampleIndex);
                System.out.println(SamplingIdx);
                System.out.println(topicNo);
                break;
            }
            this.subscribeTopic(topicToSubscribe); // Subscribe to the topic to which the row/col to be sampled belongs to

            //Currently sending the sample request to the first member in the topic as we need to make changes in the row/col distributor
            //Here we will need find the destId form the row or colNo we calculated;----------------------------------------------
            BigInteger destId = ((GossipSubProtocol) (topicToSubscribe.topicMembers.get(0).getProtocol(gossipSubId))).nodeId;
//
//            System.out.println(destId);
//            System.out.println(SamplingIdx);
             topicToSubscribe = CustomDistribution.topics.get("Topic-" + (1));
//            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(SamplingIdx),rowOrColDecider==0,rowOrColNo);
            Message sampleReqMes = createMessage(-1, Message.MSG_SAMPLE_DATA_REQUEST, destId, topicToSubscribe.topicID, Integer.toString(0),rowOrColDecider==0,0);
            this.publishMessage(sampleReqMes,destId,gossipSubId);
        }
    }

    public void handleSampleRequest(Message m, int myPid) //Sending the requested sample data if the current node has it
    {
//        System.out.println("Request received for sample");
       boolean requestedRow = m.isRow;
       int rowOrColNumb = m.rowOrColumnNumber;
       int idx = Integer.parseInt((String) m.body);
//        System.out.println(this.nodeId);
//        System.out.println(idx);
        BigInteger blockProposerId = ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId;
        for (Map.Entry<Long, Message> msg : messageCache.entrySet())
        {
            Message curMessage = msg.getValue();
//            System.out.println("*****");
//            System.out.print(curMessage.src.equals(blockProposerId));
//            System.out.print(curMessage.isRow==m.isRow);
//            System.out.print(curMessage.rowOrColumnNumber==m.rowOrColumnNumber);
//            System.out.println(curMessage.rowOrColumnNumber+ " "+m.rowOrColumnNumber);
//            System.out.println("*****");
            if (curMessage.src.equals(blockProposerId) && curMessage.isRow==m.isRow && curMessage.rowOrColumnNumber==m.rowOrColumnNumber) // Checking if the message is not sent by the block proposer
            {
                System.out.println("yes inside");
                int[] data = (int[]) curMessage.body;
                int sampleData = data[idx];
                Message sampleResponse = createMessage(m.id,Message.MSG_SAMPLE_DATA_RESPONSE,m.src,m.messageTopicID,Integer.toString(sampleData) ,m.isRow,m.rowOrColumnNumber);
                this.publishMessage(sampleResponse,m.src,gossipSubId);
                break;
            }
        }

    }

    public void handleSampleResponse(Message m,int myPid)
    {
        if(m.body!=null)
        {
            System.out.println("Yahoooo! got the sample data");
            System.out.println(m.body);
        }
    }

    @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        // System.out.println("Process event");
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

            case Message.MSG_START_SAMPLING:
                m = (Message) event;
                sampleData();
                break;

            case Message.MSG_BLOCK_PROPOSER:
                // System.out.println("I am the block producer");
                m = (Message) event;
                blockProducer();
                break;

            case Message.MSG_SAMPLE_DATA_REQUEST:
                m = (Message) event;
                handleSampleRequest(m,myPid);
                break;

            case Message.MSG_SAMPLE_DATA_RESPONSE:
                m = (Message) event;
                handleSampleResponse(m,myPid);
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
        System.out.println(Configuration.getInt("NUMBER_COPIES_DISTRIBUTED"));
        int rowNumber = 0;
        int columnNumber = 0;

        Block b = new Block(512, 512); // Creating a block

        GossipSubProtocol iGossipBlockProposer = (GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId)); // Get the protocol instance of the block proposer
        
        System.out.println("Block propsoser id is ------:" + iGossipBlockProposer.getNodeId());
        // int cnt1 = 0;
        
        for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) // Looping over all the topics
        {
            int cnt = 0;
            // System.out.println("Topic ID"+ topicEntry.getValue().topicID);
            for (Node n : topicEntry.getValue().topicMembers) // Looping over all the nodes in a given topic
            {
                // cnt1++;

                if (cnt < 8) // Allocating first 8 nodes in the topic with rows
                {
                    int[] rowToSend = b.getRowData(rowNumber); // row to be sent

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
//                    System.out.println("Row number is "+rowNumber);
                     Message newMessage = new Message(3, rowToSend,true,rowNumber);
                     newMessage.src = this.getNodeId();
                     newMessage.messageTopicID = topicEntry.getValue().topicID;
                     // System.out.println("NodeId for given topic is:"+destID);
                     newMessage.dest = destID;

//                    Message newMessage = this.createMessage(-1,Message.MSG_DATA,destID, null, rowToSend);
                    // System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);

                    this.publishMessage(newMessage, destID, gossipSubId); // Block proposer sending the row data to validator node
                    rowNumber++;
                    cnt++;
                } else // Allocating the rest 8 nodes in the topic with columns
                {
                    if (columnNumber == 512) // added this as it causing indexoverflow error as there is a bug in row/col allocation
                    {
                        continue;
                    }
                    int[] colToSend = b.getColumnData(columnNumber);

                    BigInteger destID = ((GossipSubProtocol) n.getProtocol(gossipSubId)).getNodeId();
                     Message newMessage = new Message(3, colToSend,false,columnNumber);
                     newMessage.src = this.getNodeId();
                     newMessage.messageTopicID = topicEntry.getValue().topicID;
                    // System.out.println("NodeId for given topic is:"+destID);
                     newMessage.dest = destID;
                    // System.out.println("Message with id: " + newMessage.id + " belongs to " + newMessage.dest);
                    


//                    Message newMessage = this.createMessage(-1,Message.MSG_DATA,destID, null, colToSend);

                    this.publishMessage(newMessage, destID, gossipSubId);
                    // System.out.println("NodeId for given topic is:"+destID);
                    columnNumber++;
                    cnt++;

                }
            }
        }
        // System.out.println("********Count in block producer is :" + cnt1);
        // System.out.println("*********Block proposer has sent the messages ********");
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
