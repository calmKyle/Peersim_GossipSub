package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;

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

    private int degree;
    private int degreeLow;
    private int degreeHigh;

    // Maps each topic to a set of peer IDs that are part of its mesh.
    // This represents the direct connections in the local mesh.
    private Map<String, Set<Integer>> localMesh;

    // The message cache stores recent messages
    //Only add messages with data
    //All the messages(IHAVE,IWANT,DATA) have the same message ID for the same data, so it is easy to compare/store in messageCache
    private LinkedHashMap<Long, Message> messageCache; //<MessageID,Message>


    //Maximum size of messageCashe
    public static final int MESSAGE_CACHE_SIZE = 1024; //Just took a random number atm. May need to update it--------------------------------

    private float heartbeatInitialDelay;
    private int heartbeatInterval;

    //Stores the topic ids of the which it is associated with
    private Set<Topic> subscribedTopics;

    //List of peers with their topic IDS
    private Map<BigInteger, Set<String>> peers;  //Map<peerID,Set<TopicID>>

    //List of all the nodes who have one or more common topicIDs with ours
    private Map<BigInteger, Set<String>> nodes; //<Node ID,Set<List of their subscribed topic>>

    public Object clone() {
        GossipSubProtocol cln = new GossipSubProtocol(GossipSubProtocol.prefix);
        return cln;
    }

    public GossipSubProtocol(String prefix) {
        this.degree = 0;
        this.degreeLow = 0;
        this.degreeHigh = 0;

        this.localMesh = new HashMap<>();

        this.messageCache =new LinkedHashMap<>();

        this.heartbeatInitialDelay = 0.0f;
        this.heartbeatInterval = 0;

        this.subscribedTopics = new HashSet<>();

        this.peers = new HashMap<>();

        this.nodes = new HashMap<>();

        this.nodeId = null;

        GossipSubProtocol.prefix = prefix;

        tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
    }

    //Topic subscription function
    public void subscribeTopic(Topic topic) {
        if (subscribedTopics.contains(topic)) {
            return;
        }
//        System.out.println("Node with ID: "+this.getNodeId()+" Subscribed to topic: "+topic.topicID);
        subscribedTopics.add(topic);
    }

    //Topic unsubscription function
    public void unsubscribeTopic(Topic topicID) {
        if (subscribedTopics.contains(topicID)) {
            subscribedTopics.remove(topicID);
            return;
        }
    }

    public void sendMessage(Message m, BigInteger destId, int myPid) {
//        System.out.println("Sending the message to nodeID-1:"+destId);
        Node src = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(destId);

//        System.out.println("Sending the message to nodeID-2:"+dest.getID());

        transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        transport.send(src, dest, m, gossipSubId); //========THIS LINE IS CAUSING THE ERROR========

//        System.out.println("Transport protocol class: " + Network.prototype.getProtocol(tid).getClass().getName());
        this.messageCache.put(m.id, m);

        long latency = transport.getLatency(src, dest); // Calculate message delay

        // Schedule the delivery of the message to the destination node
        EDSimulator.add(latency, m, dest, myPid);
        //May add timeout as in kademlia
    }

    //Send Message to all nodes in topic
    public void sendMessageToTopicNodes(Message m, int myPid, String topicID, Map<BigInteger, Set<String>> topicNodes) {
        //Ask:--I have created a static HashMap in the initialiser is that fine.-------------------------
//        System.out.println("Sending the message to topic nodes hurry!!!!!");
        for (Map.Entry<BigInteger, Set<String>> peerEntry : topicNodes.entrySet()) {
            BigInteger peerId = peerEntry.getKey(); // Peer ID
            Set<String> peerTopics = peerEntry.getValue(); // Topics the peer is subscribed to

            if (peerTopics.contains(topicID)) {
                m.dest = peerId;
                sendMessage(m, peerId, myPid);
            }
        }
    }

    //Send message to all the peers/local mesh in the given topic
    public void sendMessageToPeers(Message m, int myPid, String topicID) {
        sendMessageToTopicNodes(m, myPid, topicID, this.peers);
    }

    //Send the Metadata all the nodes/global mesh in the topic
    //Implement that a random no of nodes are selected for gossiping-------------------
    public void gossipMessageToTopicNodes(Message m, int myPid, String topicID) {
        sendMessageToTopicNodes(m, myPid, topicID, this.nodes);
    }

    //Function to check if the message has been seen before
    public boolean isMessageSeen(Message m) {
        if (messageCache.containsKey(m.id)) {
            return true;
        }
        return false;
    }

    public void handleIHave(Message m, int myPid) {
        if (isMessageSeen(m))//Message already present in the cache / already seen
        {
            return;
        } else {
            //Create IWANT Request Message to request the data
            //Keeping the id of the new message same as the IHave message
            Message request = new Message(m.id, Message.MSG_IWANT);
            request.src = this.nodeId;
            request.dest = m.src;
            request.messageTopicID = m.messageTopicID;

            sendMessage(request, m.src, myPid);
        }
    }

    //Send data to the node who requested/sent IWant message
    //Compare the Iwant messages with the requested data message and send that
    public void handleIWANT(Message m, int myPid) {
        if (messageCache.containsKey(m.id))
        {
            //Create DATA/response Message
            Message response = new Message(m.id, Message.MSG_DATA);
            response.src = this.nodeId;
            response.dest = m.src;
            response.body = messageCache.get(m.id).body;
            response.messageTopicID = m.messageTopicID;

            // send data/response to IWANT Request
            sendMessage(response, m.src, myPid);
        }
    }

    //This function handles the data received
    public void handleData(Message m, int myPid) {
//        System.out.println("Recieved data from nodeID: "+ m.src +" to "+this.getNodeId()+" ==="+ m.dest);
        if (!messageCache.containsKey(m.id)) //Checking the received data is already present
        {
            if(messageCache.size()>MESSAGE_CACHE_SIZE)//Checking if the size of cache has exceeded the max value
            {
                Long firstMessage = messageCache.keySet().iterator().next();
                messageCache.remove(firstMessage); //Removing the first/oldest message if the cache is full
            }

            messageCache.put(m.id, m); // If the received message isn't present then ad it to the cache

//              Commented out as there is no local mesh at the moment
//            //Sending the message with data to all the peers/local mesh
//            Message responseWithData = new Message(m.id, Message.MSG_DATA);
//            responseWithData.src = this.nodeId;
//            responseWithData.dest = m.dest; //Temporalily setting the dummy .dest. It will set correctly in the sendMessageToPeers Function
//            responseWithData.body = messageCache.get(m.id).body;
//            responseWithData.messageTopicID = m.messageTopicID;
//            sendMessageToPeers(responseWithData, myPid, m.messageTopicID);

            //sending the metadata to all the nodes in this topic
            Message responseWithMetaData = new Message(m.id, Message.MSG_IHAVE);
            responseWithMetaData.src = this.nodeId;
            responseWithMetaData.dest = m.dest; //Temporalily setting the dummy .dest. It will set correctly in the gossipMessageToTopicNodes Function
            responseWithMetaData.messageTopicID = m.messageTopicID;
//            gossipMessageToTopicNodes(responseWithMetaData, myPid, m.messageTopicID);
        }
    }

    @Override
    public void processEvent(Node myNode, int myPid, Object event) {
//        System.out.println("Process event");
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
                System.out.println("Received the data");
                m = (Message) event;
                handleData(m, myPid);
                break;

            case Message.MSG_EMPTY:
                break;
            //TO DO

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

    public BigInteger getNodeId(){
        return this.nodeId;
    }

    //Do all the nodes in the mesh send IHave message for the same messgae/Date? How? what is added in the queue?How big is it? QUESTION

}

//Compile command; Open terminal in src dir.
//javac -cp "../lib/peersim-1.0.5.jar;../lib/other-dependency.jar" -d ../classes peersim\GossipSub\*.java
//Run the stimulator; open terminal in PROJECT directory
// java -cp "classes;lib\peersim-1.0.5.jar;lib\jep-2.3.0.jar;lib\djep-1.0.0.jar" peersim.Simulator Config1.cfg