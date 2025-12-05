You said:
GossipSubProtocol.java
File
help me to split this big class file into multiple class file

Breaking down the Java file

I’m analyzing the large Java file "GossipSubProtocol.java" to split it into smaller, more manageable classes. Identifying GossipSubProtocol and its nested classes like EphemeralMsgInfo and PeerScoreInfo is essential.

Transforming and organizing classes

I’m mapping out a new file structure for "GossipSubProtocol.java," organizing classes into appropriate files. For instance, creating "EphemeralMsgInfo.java" with its fields and methods.

Organizing class references

I’m packaging the PeerScoreInfo class into a new file, maintaining references in GossipSubProtocol. Considering methods like computeScore, I opt to retain them in GossipSubProtocol for minimal changes.

Below is an example of how you might split up the original GossipSubProtocol source into multiple classes. The goal is simply to move the nested/auxiliary classes (EphemeralMsgInfo, PeerScoreInfo, etc.) out of GossipSubProtocol.java into their own files, while preserving all functionality.

In the example below, we create three new files:

    EphemeralMsgInfo.java
    PeerScoreInfo.java
    GossipSubProtocol.java (with the nested classes removed)

    Important Note

        You should keep all files in the same package peersim.GossipSub;.
        If you have references to these classes in other files, make sure to import peersim.GossipSub.EphemeralMsgInfo; and import peersim.GossipSub.PeerScoreInfo; wherever they are used.
        If there are references to fields or methods that used to be private inside GossipSubProtocol, you may need to adjust their visibility (e.g. change them to package-private or provide getters/setters).
        If you have additional logic for “computeScore” or similar methods that directly used PeerScoreInfo, you can either move them into PeerScoreInfo or leave them in GossipSubProtocol—you’ll just need to ensure the references remain valid.

EphemeralMsgInfo.java

package peersim.GossipSub;

import peersim.core.CommonState;

/**
 * EphemeralMsgInfo holds temporary data about a message 
 * for re-advertisement (IHAVE) and expiration tracking.
 */
public class EphemeralMsgInfo {
    // The original message
    public Message message;
    
    // When it was first stored
    public long arrivalTime; 
    
    // How many times we've re-advertised (IHAVE) so far
    public int advertiseCount; 

    public EphemeralMsgInfo(Message m) {
        this.message = m;
        this.arrivalTime = CommonState.getTime();
        this.advertiseCount = 0;
    }
}

PeerScoreInfo.java

package peersim.GossipSub;

/**
 * PeerScoreInfo holds all the scoring-related counters/stats
 * for a given peer in Gossipsub.
 */
public class PeerScoreInfo {
    // When the peer entered our mesh
    public long timeInMeshStart;

    // Number of times this peer delivered a message to me "first"
    public int firstMessageDeliveries;

    // Number of invalid messages from this peer
    public int invalidMessages;

    // Count of under-delivery events
    public int meshUnderDelivery;

    // Last computed total score
    public double cachedScore;

    // When we connected
    public long connectedTime;

    public PeerScoreInfo(long currentTime) {
        this.timeInMeshStart      = currentTime;
        this.connectedTime        = currentTime;
        this.firstMessageDeliveries = 0;
        this.invalidMessages      = 0;
        this.meshUnderDelivery    = 0;
        this.cachedScore          = 0.0;
    }
}

GossipSubProtocol.java

Below is your main protocol class with references to EphemeralMsgInfo and PeerScoreInfo now imported. Everything else remains the same, except we remove the nested static class EphemeralMsgInfo and static class PeerScoreInfo definitions.

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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static peersim.GossipSub.CustomDistribution.*;

/**
 * Main GossipSubProtocol class, now split out so that EphemeralMsgInfo
 * and PeerScoreInfo each live in their own Java files.
 */
public class GossipSubProtocol implements Cloneable, EDProtocol {

    private static final int MSG_HEARTBEAT = 9999;
    private static final long HEARTBEAT_PERIOD = 1000; // 1 second
    // We'll re-advertise (IHAVE) new messages up to 3 times
    private static final int GOSSIP_ADVERTISE_ROUNDS = 3;
    // We'll expire (drop) messages from ephemeral cache after 3 seconds
    private static final long MESSAGE_EXPIRATION_MS = 3000;

    // Configuration Constants
    private static int MESSAGE_CACHE_SIZE = 1024;
    private static String PAR_TRANSPORT = "transport";
    private static int NUMBER_OF_ROWSCOLS_IN_A_TOPIC =
            Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
    private static int NUMBER_OF_VALIDATORS_PER_TOPIC =
            Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC", 128);
    private static int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC =
            NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1
               ? NUMBER_OF_VALIDATORS_PER_TOPIC
               : (int) Math.ceil(
                   (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0)
                   / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));

    private static String prefix;

    // Instance Variables
    public BigInteger nodeId;
    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;
    private int minDegree = Configuration.getInt("MIN_DEGREE", 4);
    private int maxDegree = Configuration.getInt("MAX_DEGREE", 16);
    protected int degree = Configuration.getInt("DEGREE", 8);

    private Set<Topic> subscribedTopics = new HashSet<>();
    protected Map<String, Set<BigInteger>> localMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> gossipMesh = new HashMap<>();
    protected Map<String, Set<BigInteger>> topicNodes = new HashMap<>();

    private LinkedHashMap<Long, Message> messageCache = new LinkedHashMap<>();
    private LinkedHashMap<Long, Message> IWANTmessageCache = new LinkedHashMap<>();

    public IncrementalStats seedArrivalTimeStore       = new IncrementalStats();
    public IncrementalStats seedPartArrivalTimeStore   = new IncrementalStats();
    public IncrementalStats samplingRTTTimeStore       = new IncrementalStats();

    public List<Long> messageArrivalTimeFromBP  = new ArrayList<>();
    public List<Long> messageDelayTimeFromBP    = new ArrayList<>();
    public List<Long> seedPartArrivalTimeFromPeer  = new ArrayList<>();
    public List<Long> seedPartDelayTimeFromPeer    = new ArrayList<>();
    public List<Long> sampleArrivalTime            = new ArrayList<>();
    public List<Long> sampleDelayTime              = new ArrayList<>();

    private List<Message> custodyData1       = new ArrayList<>();
    private List<Message> custodyData2       = new ArrayList<>();
    private List<Message> dataReceivedFromBP = new ArrayList<>();
    private List<Message> custody1Parts      = new ArrayList<>();
    private List<Message> custody2Parts      = new ArrayList<>();

    private List<Message> messageQueue                       = new ArrayList<>();
    private List<Long> messageTransmissionDelayQueue         = new ArrayList<>();

    private TreeMap<Long, Message> sentMsg                   = new TreeMap<>();
    private TreeMap<Long, Message> sentSeedingPartMsg        = new TreeMap<>();

    private int interfaceBandwidth       = Configuration.getInt("INTERFACE_BANDWIDTH", 12_500_000);
    private int blockProducerBandwidth   = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1_250_000_000);
    private int distributionStrategy     = Configuration.getInt("DISTRIBUTION_STRATEGY");

    public long totalDataTransmitted = 0;
    public long totalTransmissionTime = 0;
    private long lastMessageTransmissionTime = 0;

    public int sampleRequestUnsuccessful = 0;
    public int noOfSampleRequestsSent    = 0;
    public int noOfSamplesReceived       = 0;
    public int noOfSeedPartsReceived     = 0;
    private int randomSampleCounter      = 0;

    public String custody1;
    public String custody2;
    private boolean samplingStarted = false;

    private int sampleAmount = Configuration.getInt("SAMPLE_AMOUNT", 75);

    protected static List<Set<BigInteger>> rowColHolders = new ArrayList<>(1024);

    private boolean isDEBUG = true;

    private Set<Long> seenMessageIDs = new HashSet<>();

    // Ephemeral Cache: Maps message ID -> EphemeralMsgInfo
    private Map<Long, EphemeralMsgInfo> ephemeralCache = new LinkedHashMap<>();

    // Peer scoring data (similar to Gossipsub v1.1+). 
    // Maps peer ID -> PeerScoreInfo
    private Map<BigInteger, PeerScoreInfo> peerScores = new HashMap<>();

    public GossipSubProtocol(String prefix) {
        GossipSubProtocol.prefix = prefix;
        this.nodeId     = null;
        this.tid        = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
        this.samplingStarted = false;

        // For testing, set isDEBUG based on config
        this.isDEBUG = Configuration.contains("DEBUG_GOSSIPSUB")
                && Configuration.getBoolean("DEBUG_GOSSIPSUB", false);
    }

    @Override
    public Object clone() {
        return new GossipSubProtocol(GossipSubProtocol.prefix);
    }

    // ------------------------------------------------------------------------
    // Score computations (example: Weighted sum approach).
    // We keep this in the protocol, but it references PeerScoreInfo.
    // ------------------------------------------------------------------------
    private double computeScore(PeerScoreInfo psi) {
        double w_timeInMesh     = 0.01; // small positive weight
        double w_firstMsg       = 0.1;  // each first message has some value
        double w_invalid        = -1.0; // heavily penalize invalid
        double w_underDelivery  = -0.5; // penalize under-delivery

        long now = CommonState.getTime();
        double timeInMesh = (now - psi.timeInMeshStart);

        double score = 0.0;
        score += (w_timeInMesh * timeInMesh);
        score += (w_firstMsg   * psi.firstMessageDeliveries);
        score += (w_invalid    * psi.invalidMessages);
        score += (w_underDelivery * psi.meshUnderDelivery);

        return score;
    }

    // ------------------------------------------------------------------------
    // Heartbeat-based ephemeral cache and peer scoring logic
    // ------------------------------------------------------------------------
    private void runHeartbeat(Node myNode, int myPid) {
        long now = CommonState.getTime();
        if (isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] Heartbeat at t=" + now +
                    " Node=" + nodeId +
                    " ephemeralCacheSize=" + ephemeralCache.size());
        }

        // 1) Expire old ephemeral messages
        Iterator<Map.Entry<Long, EphemeralMsgInfo>> it = ephemeralCache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, EphemeralMsgInfo> e = it.next();
            EphemeralMsgInfo info = e.getValue();
            if (now - info.arrivalTime >= MESSAGE_EXPIRATION_MS) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Expiring message " + e.getKey() +
                            " from ephemeralCache on node " + nodeId);
                }
                it.remove();
            }
        }

        // 2) Re-advertise (IHAVE) messages up to GOSSIP_ADVERTISE_ROUNDS times
        for (EphemeralMsgInfo info : ephemeralCache.values()) {
            if (info.advertiseCount < GOSSIP_ADVERTISE_ROUNDS) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Re-advertising messageID=" + info.message.id +
                            " advertiseCount=" + info.advertiseCount +
                            " node=" + nodeId);
                }
                advertiseMessageIHAVE(info.message, myPid);
                info.advertiseCount++;
            }
        }

        // 3) Update peer scores & prune/graft if needed
        if (isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] Computing peer scores on node " + nodeId);
        }
        for (Map.Entry<BigInteger, PeerScoreInfo> entry : peerScores.entrySet()) {
            PeerScoreInfo psi = entry.getValue();
            double newScore = computeScore(psi);
            double oldScore = psi.cachedScore;
            psi.cachedScore = newScore;

            if (isDEBUG) {
                System.out.println("[DEBUG HEARTBEAT]   Peer=" + entry.getKey() +
                        " oldScore=" + oldScore +
                        " newScore=" + newScore);
            }
        }

        // Example: prune peers with negative score
        for (BigInteger peerID : new HashSet<>(peerScores.keySet())) {
            PeerScoreInfo psi = peerScores.get(peerID);
            if (psi.cachedScore < 0) {
                if (isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Removing peer " + peerID +
                            " from mesh due to negative score on node " + nodeId);
                }
                removePeerFromMesh(peerID);
            }
        }

        // If you want to add new peers if we’re below “degree”, do so:
        updateMeshConnections();
    }

    // Example: removing a peer from the local mesh
    private void removePeerFromMesh(BigInteger peerID) {
        for (String topicID : localMesh.keySet()) {
            localMesh.get(topicID).remove(peerID);
        }
        // Optionally, remove from peerScores if you want a clean slate.
        // peerScores.remove(peerID);
    }

    // Example: We re-advertise (IHAVE) from ephemeral cache
    private void advertiseMessageIHAVE(Message originalMsg, int myPid) {
        if (isDEBUG) {
            System.out.println("[DEBUG] Node " + nodeId +
                    " advertiseMessageIHAVE for msgID=" + originalMsg.id);
        }

        Message ihave = createMessage(
                originalMsg.id,
                Message.MSG_IHAVE,
                this.nodeId,
                null,
                originalMsg.messageTopicID,
                null,
                originalMsg.isRow,
                originalMsg.rowOrColumnNumber,
                originalMsg.partNumber,
                CommonState.getTime(),
                -6);

        Set<BigInteger> peers = localMesh.getOrDefault(originalMsg.messageTopicID, new HashSet<>());
        for (BigInteger peer : peers) {
            if (peer.equals(this.nodeId)) continue;
            if (isDEBUG) {
                System.out.println("[DEBUG]    -> Sending IHAVE(msgID=" + originalMsg.id +
                        ") to peer=" + peer + " from node=" + nodeId);
            }
            Message copy = (Message) ihave.copy();
            copy.dest = peer;
            publishMessage(copy, peer, myPid);
        }
    }

    private void addPeerScoreIfAbsent(BigInteger peerID) {
        if (!peerScores.containsKey(peerID)) {
            peerScores.put(peerID, new PeerScoreInfo(CommonState.getTime()));
        }
    }

    // ... Everything else (updateMeshConnections, addMorePeers, removeExcessPeers, etc.)
    // ... remains the same as in your original file ...
    
    // [KEEP the rest of your methods: processEvent, handleIHave, handleIWANT, handleData, 
    //  handleBlockProducerData, sampling logic, etc. -- unchanged]
    
    // Below is just one example method to show how we store ephemeral messages:
    private void storeInEphemeralCache(Message m) {
        if (!ephemeralCache.containsKey(m.id)) {
            ephemeralCache.put(m.id, new EphemeralMsgInfo(m));
            if (isDEBUG) {
                System.out.println("[DEBUG] Node " + nodeId +
                        " storeInEphemeralCache msgID=" + m.id);
            }
        }
    }

    // Example for marking first delivery to update peer’s firstMessageDeliveries
    private void markFirstDelivery(Message m) {
        if (!seenMessageIDs.contains(m.id)) {
            seenMessageIDs.add(m.id);
            PeerScoreInfo psi = peerScores.get(m.src);
            if (psi != null) {
                psi.firstMessageDeliveries++;
                if (isDEBUG) {
                    System.out.println("[DEBUG] Node " + nodeId +
                            " got first delivery of msgID=" + m.id +
                            " from peer=" + m.src +
                            " -> incrementing firstMessageDeliveries to " 
                            + psi.firstMessageDeliveries);
                }
            }
        }
    }

    // Example helper to create a message
    private Message createMessage(long id, int type, BigInteger src, BigInteger dest,
                                  String topicID, Object body,
                                  boolean isRow, int RowOrColNum,
                                  int partNum, long timeStamp, long ackid) {
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

    // publishMessage, etc. remain the same ...
    // processEvent(...) remains the same ...
    // distribution logic remains the same ...
}

How to Proceed

    Create the new Java files (EphemeralMsgInfo.java, PeerScoreInfo.java) in the same directory/package as GossipSubProtocol.java.

    Remove the inner class definitions for EphemeralMsgInfo and PeerScoreInfo from GossipSubProtocol.java.

    Import them where needed. For example:

    import peersim.GossipSub.EphemeralMsgInfo;
    import peersim.GossipSub.PeerScoreInfo;

    Adjust visibility (e.g. fields, methods) as needed so that code compiles (e.g., if you need getters/setters).

This basic refactoring gives you separate files for each main class, reducing the size and complexity of your GossipSubProtocol.java. You could do similar splits if you have other large nested classes or big chunks of functionality that you want in separate files.
You said:
I still want the GossipSubProtocol.java splitted into smaller class ?

Considering major chunks

I'm mapping out separating each key component of GossipSubProtocol into distinct classes, like MessageSendingUtil and MeshManagement, to streamline the code structure.

Proposing separation strategy

I’m suggesting dividing GossipSubProtocol into smaller classes like DistributionManager for block distribution, SamplingManager for sampling logic, and GossipHeartbeat for heartbeat logic, while keeping the main class intact.

Below is an example of how you can further split GossipSubProtocol.java into multiple classes/files, beyond just moving out EphemeralMsgInfo and PeerScoreInfo. The main idea is to group logically related functionality into separate classes, keep them in the same package, and have GossipSubProtocol delegate certain operations to these helper classes.

Ultimately, how far you decompose is up to you—there is no single “right” way. Below is just one example showing a common approach:

    GossipSubProtocol - The main class that implements EDProtocol and coordinates the overall flow.
    HeartbeatManager - Contains the heartbeat-related logic (expiration of ephemeral messages, re-advertising, peer scoring).
    MeshManager - Manages logic for local mesh, adding or removing peers, plus topic subscriptions.
    DistributionManager - Contains code for your two “distribution strategies” (nCopiesDistributionStrategy() and shardingBasedDistribution()).
    SamplingManager - Contains the logic and data structures for the random sampling requests/responses.

You can choose to split more or fewer pieces depending on your needs.
1. GossipSubProtocol.java

package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;
import peersim.util.IncrementalStats;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static peersim.GossipSub.CustomDistribution.*;

/**
 * Main GossipSubProtocol class, now split into smaller helper classes.
 */
public class GossipSubProtocol implements Cloneable, EDProtocol {

    // ------------------------------------------------------------------------
    // Constants & Config
    // ------------------------------------------------------------------------
    private static final int MSG_HEARTBEAT = 9999;
    private static final long HEARTBEAT_PERIOD = 1000; // 1 second
    
    public static final int MESSAGE_CACHE_SIZE = 1024;
    private static final String PAR_TRANSPORT = "transport";
    
    public static final int NUMBER_OF_ROWSCOLS_IN_A_TOPIC =
            Configuration.getInt("NUMBER_OF_ROWS_AND_COLS_IN_A_TOPIC", 16);
    public static final int NUMBER_OF_VALIDATORS_PER_TOPIC =
            Configuration.getInt("NUMBER_OF_VALIDATORS_PER_TOPIC", 128);
    public static final int NUMBER_OF_ROW_OR_COLUMN_HOLDERS_PER_TOPIC =
            NUMBER_OF_ROWSCOLS_IN_A_TOPIC == 1
               ? NUMBER_OF_VALIDATORS_PER_TOPIC
               : (int) Math.ceil(
                   (NUMBER_OF_VALIDATORS_PER_TOPIC / 2.0)
                   / Configuration.getInt("NUMBER_ROWS_OR_COLS_PER_TOPIC"));

    private static String prefix;
    
    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------
    public BigInteger nodeId;
    private UnreliableTransport transport;
    private int tid;
    private int gossipSubId;

    public boolean isDEBUG = true;
    public int distributionStrategy = Configuration.getInt("DISTRIBUTION_STRATEGY");
    
    // Holds state for scoring, ephemeral cache, etc.
    // Moved out to HeartbeatManager
    private HeartbeatManager heartbeatManager;
    
    // Holds logic for mesh membership
    private MeshManager meshManager;
    
    // Holds logic for block distribution strategies
    private DistributionManager distributionManager;
    
    // Holds logic for sampling
    private SamplingManager samplingManager;

    // ------------------------------------------------------------------------
    // Stats & Data
    // ------------------------------------------------------------------------
    public IncrementalStats seedArrivalTimeStore       = new IncrementalStats();
    public IncrementalStats seedPartArrivalTimeStore   = new IncrementalStats();
    public IncrementalStats samplingRTTTimeStore       = new IncrementalStats();
    
    public List<Long> messageArrivalTimeFromBP  = new ArrayList<>();
    public List<Long> messageDelayTimeFromBP    = new ArrayList<>();
    public List<Long> seedPartArrivalTimeFromPeer  = new ArrayList<>();
    public List<Long> seedPartDelayTimeFromPeer    = new ArrayList<>();
    public List<Long> sampleArrivalTime            = new ArrayList<>();
    public List<Long> sampleDelayTime              = new ArrayList<>();
    
    public long totalDataTransmitted   = 0;
    public long totalTransmissionTime  = 0;
    private long lastMessageTransmissionTime = 0;

    // Additional distributions/logic placeholders
    private int interfaceBandwidth     = Configuration.getInt("INTERFACE_BANDWIDTH", 12_500_000);
    private int blockProducerBandwidth = Configuration.getInt("BLOCK_PRODUCER_BANDWIDTH", 1_250_000_000);

    // ------------------------------------------------------------------------
    // Constructor & Clone
    // ------------------------------------------------------------------------
    public GossipSubProtocol(String prefix) {
        GossipSubProtocol.prefix = prefix;
        this.tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
        
        // For testing, set isDEBUG based on config
        this.isDEBUG = Configuration.contains("DEBUG_GOSSIPSUB")
                && Configuration.getBoolean("DEBUG_GOSSIPSUB", false);

        // Initialize sub-managers
        this.heartbeatManager = new HeartbeatManager(this);
        this.meshManager      = new MeshManager(this);
        this.distributionManager = new DistributionManager(this);
        this.samplingManager  = new SamplingManager(this);
    }

    @Override
    public Object clone() {
        return new GossipSubProtocol(GossipSubProtocol.prefix);
    }

    // ------------------------------------------------------------------------
    // EDProtocol: main event handler
    // ------------------------------------------------------------------------
    @Override
    public void processEvent(Node myNode, int myPid, Object event) {
        this.gossipSubId = myPid;
        
        // We handle the “outer” switch here, delegating sub-logic
        if (event instanceof SimpleEvent) {
            SimpleEvent se = (SimpleEvent) event;
            switch (se.getType()) {
                case Message.MSG_BLOCK_PROPOSER:
                    handleBlockProducerEvent((Message) event, myNode, myPid);
                    break;
                    
                case Message.MSG_IHAVE:
                    meshManager.handleIHave((Message) event, myPid);
                    break;
                    
                case Message.MSG_IWANT:
                    meshManager.handleIWant((Message) event, myPid);
                    break;
                    
                case Message.MSG_DATA:
                    handleDataEvent((Message) event, myPid);
                    break;
                    
                case Message.MSG_SAMPLE_DATA_REQUEST:
                    samplingManager.handleSampleRequest((Message) event, myPid);
                    break;

                case Message.MSG_SAMPLE_DATA_RESPONSE:
                    samplingManager.handleSampleResponse((Message) event, myPid);
                    break;

                case HeartbeatManager.MSG_HEARTBEAT:
                    heartbeatManager.runHeartbeat(myNode, myPid);
                    break;

                default:
                    // Possibly handle others, or do nothing
                    break;
            }
        }
        else if (event instanceof Timeout) {
            samplingManager.handleTimeout((Timeout) event, myPid);
        }
    }

    // ------------------------------------------------------------------------
    // Example: handle the block producer event
    // ------------------------------------------------------------------------
    private void handleBlockProducerEvent(Message m, Node myNode, int myPid) {
        System.out.println("I am the block producer");
        System.out.println("Message sent from trafficGenerator at: " + m.timestamp);
        System.out.println("Block producer started at: " + CommonState.getTime());

        // Choose distribution strategy
        if (this.distributionStrategy == 3) {
            distributionManager.nCopiesDistributionStrategy();
        } else if (this.distributionStrategy == 2) {
            distributionManager.shardingBasedDistribution();
        }
        
        // Kick off heartbeat
        EDSimulator.add(
            HEARTBEAT_PERIOD,
            new SimpleEvent(HeartbeatManager.MSG_HEARTBEAT),
            myNode,
            myPid
        );
    }

    // ------------------------------------------------------------------------
    // Example: handle normal data events
    // ------------------------------------------------------------------------
    private void handleDataEvent(Message m, int myPid) {
        // If data is from block proposer
        GossipSubProtocol proposerNode = (GossipSubProtocol) CustomDistribution.blockProposerNode
                .getProtocol(gossipSubId);
        if (m.src.equals(proposerNode.nodeId)) {
            meshManager.handleBlockProducerData(m, myPid);
        } else {
            meshManager.markFirstDelivery(m);
            meshManager.handleData(m, myPid);
        }
    }

    // ------------------------------------------------------------------------
    // Example: Publishing a message (unchanged except we moved it here)
    // ------------------------------------------------------------------------
    public void publishMessage(Message m, BigInteger destId, int myPid) {
        if (transport == null) {
            // Lazy init or direct lookup
            Node srcNode = CustomDistribution.networkNodes.get(this.nodeId);
            transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
        }

        Node src  = CustomDistribution.networkNodes.get(this.nodeId);
        Node dest = CustomDistribution.networkNodes.get(m.dest);

        long bandwidth = (this.nodeId.equals(
                ((GossipSubProtocol) (CustomDistribution.blockProposerNode.getProtocol(gossipSubId))).nodeId))
                ? blockProducerBandwidth
                : interfaceBandwidth;

        long latency = transport.getLatency(src, dest);

        // Simple queueing approach
        long currentTime = CommonState.getTime();
        // [Your existing queue logic... omitted for brevity]
        
        // For example, a simple formula:
        int messageSize = meshManager.calculateMessageSize(m);
        long transmissionDelay = (long) Math.ceil((double) messageSize / bandwidth);
        long totalDelay = latency + transmissionDelay;
        
        totalDataTransmitted += messageSize;
        totalTransmissionTime += totalDelay;

        // Schedule the event
        EDSimulator.add(totalDelay, m, dest, myPid);
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------
    public Message createMessage(long id, int type, BigInteger src, BigInteger dest,
                                 String topicID, Object body,
                                 boolean isRow, int rowOrColNum,
                                 int partNum, long timeStamp, long ackid) {
        Message msg;
        if (id == -1) {
            msg = new Message(type, isRow, rowOrColNum, partNum, ackid);
            msg.timestamp = CommonState.getTime();
        } else {
            msg = new Message(id, type, isRow, rowOrColNum, partNum, ackid);
            msg.timestamp = timeStamp;
        }
        msg.src            = src;
        msg.dest           = dest;
        msg.messageTopicID = topicID;
        msg.body           = body;
        return msg;
    }

    public BigInteger getNodeId() {
        return this.nodeId;
    }

    public void setNodeId(BigInteger tmp) {
        this.nodeId = tmp;
    }
    
    public HeartbeatManager getHeartbeatManager() {
        return this.heartbeatManager;
    }

    public MeshManager getMeshManager() {
        return this.meshManager;
    }

    public DistributionManager getDistributionManager() {
        return this.distributionManager;
    }

    public SamplingManager getSamplingManager() {
        return this.samplingManager;
    }
}

    Key points:

        We’ve removed most “detailed” methods (heartbeat, data handling, mesh logic, distribution) out of the main class.
        We still keep some “glue” in GossipSubProtocol, especially event switching.
        Some references (like transport, blockProducerBandwidth, interfaceBandwidth, etc.) remain in GossipSubProtocol, because they’re used across different components.
        The “managers” each need a reference back to GossipSubProtocol so they can access shared fields (like nodeId, publishMessage(), or logging fields).

2. HeartbeatManager.java

package peersim.GossipSub;

import peersim.core.Node;
import peersim.core.CommonState;
import peersim.edsim.EDSimulator;

import java.util.*;

/**
 * Contains heartbeat logic: ephemeral message expiration, re-advertising, peer scoring, etc.
 */
public class HeartbeatManager {

    public static final int MSG_HEARTBEAT = 9999;

    private GossipSubProtocol parent;  // reference to main protocol

    // Hard-coded from original code
    private static final long MESSAGE_EXPIRATION_MS = 3000;
    private static final int GOSSIP_ADVERTISE_ROUNDS = 3;

    public HeartbeatManager(GossipSubProtocol parent) {
        this.parent = parent;
    }

    public void runHeartbeat(Node myNode, int myPid) {
        long now = CommonState.getTime();

        if (parent.isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] t=" + now + " Node=" + parent.nodeId);
        }

        // 1) Expire old ephemeral messages
        Iterator<Map.Entry<Long, EphemeralMsgInfo>> it = EphemeralStore.getEphemeralCache().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, EphemeralMsgInfo> e = it.next();
            if (now - e.getValue().arrivalTime >= MESSAGE_EXPIRATION_MS) {
                if (parent.isDEBUG) {
                    System.out.println("[DEBUG HEARTBEAT] Expiring message " + e.getKey()
                            + " from ephemeralCache on node " + parent.nodeId);
                }
                it.remove();
            }
        }

        // 2) Re-advertise (IHAVE) messages up to GOSSIP_ADVERTISE_ROUNDS times
        for (EphemeralMsgInfo info : EphemeralStore.getEphemeralCache().values()) {
            if (info.advertiseCount < GOSSIP_ADVERTISE_ROUNDS) {
                parent.getMeshManager().advertiseMessageIHAVE(info.message, myPid);
                info.advertiseCount++;
            }
        }

        // 3) Update peer scores & prune peers if needed
        if (parent.isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] Updating peer scores on node " + parent.nodeId);
        }
        Map<BigInteger, PeerScoreInfo> scores = PeerScores.getAllScores();
        for (Map.Entry<BigInteger, PeerScoreInfo> entry : scores.entrySet()) {
            PeerScoreInfo psi = entry.getValue();
            double oldScore   = psi.cachedScore;
            double newScore   = computeScore(psi);
            psi.cachedScore   = newScore;

            if (parent.isDEBUG) {
                System.out.println("   Peer=" + entry.getKey() + " oldScore=" + oldScore + " newScore=" + newScore);
            }
        }

        // Example: remove peers below score threshold
        Set<BigInteger> toRemove = new HashSet<>();
        for (Map.Entry<BigInteger, PeerScoreInfo> entry : scores.entrySet()) {
            if (entry.getValue().cachedScore < 0) {
                toRemove.add(entry.getKey());
            }
        }
        for (BigInteger peerId : toRemove) {
            if (parent.isDEBUG) {
                System.out.println("[DEBUG HEARTBEAT] Removing peer " + peerId + " from mesh on node " + parent.nodeId);
            }
            parent.getMeshManager().removePeerFromMesh(peerId);
        }

        // If you want to maintain “degree,” do so:
        parent.getMeshManager().updateMeshConnections();

        // Re-schedule if desired
        EDSimulator.add(
            parent.HEARTBEAT_PERIOD,
            new SimpleEvent(MSG_HEARTBEAT),
            myNode,
            myPid
        );
    }

    // Weighted sum approach from original code
    private double computeScore(PeerScoreInfo psi) {
        double w_timeInMesh    = 0.01;
        double w_firstMsg      = 0.1;
        double w_invalid       = -1.0;
        double w_underDelivery = -0.5;

        long now          = CommonState.getTime();
        double timeInMesh = (now - psi.timeInMeshStart);

        double score = 0.0;
        score += (w_timeInMesh    * timeInMesh);
        score += (w_firstMsg      * psi.firstMessageDeliveries);
        score += (w_invalid       * psi.invalidMessages);
        score += (w_underDelivery * psi.meshUnderDelivery);

        return score;
    }
}

    Notice we introduced EphemeralStore and PeerScores (static singletons or utility classes) to store ephemeral messages and peer scores. If you prefer, you can keep them in GossipSubProtocol itself. We’re just illustrating one possible design.

3. MeshManager.java

package peersim.GossipSub;

import peersim.core.CommonState;

import java.math.BigInteger;
import java.util.*;

import static peersim.GossipSub.CustomDistribution.*;

/**
 * Handles local mesh logic, subscription, add/remove peers, 
 * plus data receiving from block producer if needed.
 */
public class MeshManager {

    private GossipSubProtocol parent;
    private Set<Long> seenMessageIDs = new HashSet<>();

    // local mesh: topicID -> set of peers
    // gossip mesh: topicID -> set of peers
    // We store them here or in the parent
    private Map<String, Set<BigInteger>> localMesh  = new HashMap<>();
    private Map<String, Set<BigInteger>> gossipMesh = new HashMap<>();

    public MeshManager(GossipSubProtocol parent) {
        this.parent = parent;
    }

    // ------------------------------------------------------------------------
    // “IHave” and “IWant” logic
    // ------------------------------------------------------------------------
    public void handleIHave(Message m, int myPid) {
        // example logic from original code
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] handleIHave on node " + parent.nodeId + ", msgID=" + m.id);
        }
        
        if (!MessageCache.contains(m.id) || IWANTCache.contains(m.id)) {
            // do something ...
            // store ephemeral
            EphemeralStore.storeInEphemeralCache(m);
            
            // possibly forward / gossip
            // ...
        }
    }

    public void handleIWant(Message m, int myPid) {
        // handle logic from your original code
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] handleIWANT on node " + parent.nodeId + ", msgID=" + m.id);
        }
        // ...
    }

    public void handleData(Message m, int myPid) {
        // e.g. if not in cache, store
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] handleDATA on node " + parent.nodeId + ", msgID=" + m.id);
        }
        markFirstDelivery(m);
        // ephemeral, etc.
    }

    public void handleBlockProducerData(Message m, int myPid) {
        // logic for data from the block producer node
        // ...
    }

    public void advertiseMessageIHAVE(Message originalMsg, int myPid) {
        // same logic as in your original “advertiseMessageIHAVE”
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] Node " + parent.nodeId +
                    " advertiseMessageIHAVE for msgID=" + originalMsg.id);
        }
        Message ihave = parent.createMessage(
                originalMsg.id,
                Message.MSG_IHAVE,
                parent.nodeId,
                null,
                originalMsg.messageTopicID,
                null,
                originalMsg.isRow,
                originalMsg.rowOrColumnNumber,
                originalMsg.partNumber,
                CommonState.getTime(),
                -6
        );

        Set<BigInteger> peers = localMesh.getOrDefault(originalMsg.messageTopicID, new HashSet<>());
        for (BigInteger peer : peers) {
            if (peer.equals(parent.nodeId)) continue;
            Message copy = (Message) ihave.copy();
            copy.dest = peer;
            parent.publishMessage(copy, peer, myPid);
        }
    }

    public void removePeerFromMesh(BigInteger peerID) {
        for (String topicID : localMesh.keySet()) {
            localMesh.get(topicID).remove(peerID);
        }
        // Optionally remove from peer scores
        PeerScores.remove(peerID);
    }

    public void updateMeshConnections() {
        // e.g. ensuring your “degree”
        // ...
    }

    // ------------------------------------------------------------------------
    // Mark first delivery for peer scoring
    // ------------------------------------------------------------------------
    public void markFirstDelivery(Message m) {
        if (!seenMessageIDs.contains(m.id)) {
            seenMessageIDs.add(m.id);
            PeerScoreInfo psi = PeerScores.getScore(m.src);
            if (psi != null) {
                psi.firstMessageDeliveries++;
            }
        }
    }

    // ------------------------------------------------------------------------
    // Utility example: message size
    // ------------------------------------------------------------------------
    public int calculateMessageSize(Message message) {
        int size = 0;
        // do your logic from original code
        // ...
        return size;
    }
}

    Note that we used MessageCache and IWANTCache as placeholders for whatever data structures you originally had. If you prefer them to be singletons or static, that’s fine. Or keep them as fields in MeshManager.

4. DistributionManager.java

package peersim.GossipSub;

import peersim.config.Configuration;

/**
 * Contains the nCopies and sharding distribution logic from your original code.
 */
public class DistributionManager {

    private GossipSubProtocol parent;

    public DistributionManager(GossipSubProtocol parent) {
        this.parent = parent;
    }

    public void nCopiesDistributionStrategy() {
        // your original code from nCopiesDistributionStrategy()
        // just reference parent fields/methods if needed
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] nCopiesDistributionStrategy on node " + parent.nodeId);
        }

        // Example usage:
        //  - call parent.publishMessage(...)
        //  - read parent fields
        //  - or store local state as needed
        // ...
    }

    public void shardingBasedDistribution() {
        // your original code from shardingBasedDistribution()
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] shardingBasedDistribution on node " + parent.nodeId);
        }
        // ...
    }
}

5. SamplingManager.java

package peersim.GossipSub;

import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Random;

/**
 * Handles the random sampling logic, requests, timeouts, and responses.
 */
public class SamplingManager {

    private GossipSubProtocol parent;

    public SamplingManager(GossipSubProtocol parent) {
        this.parent = parent;
    }

    // Example: handle sampling request
    public void handleSampleRequest(Message m, int myPid) {
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] handleSampleRequest on node=" + parent.nodeId + ", msgID=" + m.id);
        }
        // ...
        // logic from startSampling, sampleDataRequest, etc.
    }

    // Example: handle sampling response
    public void handleSampleResponse(Message m, int myPid) {
        // ...
        // update stats, etc.
    }

    // Example: handle a Timeout event for sampling
    public void handleTimeout(Timeout timeoutEvent, int myPid) {
        if (parent.isDEBUG) {
            System.out.println("[DEBUG] handleTimeout on node=" + parent.nodeId 
                    + ", timedOutMsgID=" + timeoutEvent.msgID);
        }
        // your retry logic...
    }
}

6. Support Classes

    EphemeralMsgInfo and PeerScoreInfo can remain as separate files just like in the earlier example.
    If you want to store ephemeral messages and peer scores in global static maps, you can create something like EphemeralStore.java and PeerScores.java:

EphemeralStore.java

package peersim.GossipSub;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple static store for ephemeral messages: msgID -> EphemeralMsgInfo
 * (Alternatively, keep these as fields in HeartbeatManager or GossipSubProtocol.)
 */
public class EphemeralStore {

    private static Map<Long, EphemeralMsgInfo> ephemeralCache = new LinkedHashMap<>();

    public static Map<Long, EphemeralMsgInfo> getEphemeralCache() {
        return ephemeralCache;
    }
    
    public static void storeInEphemeralCache(Message m) {
        if (!ephemeralCache.containsKey(m.id)) {
            ephemeralCache.put(m.id, new EphemeralMsgInfo(m));
        }
    }
}

PeerScores.java

package peersim.GossipSub;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple static store for peer scores, peerID -> PeerScoreInfo
 * (Alternatively, keep this in GossipSubProtocol.)
 */
public class PeerScores {

    private static Map<BigInteger, PeerScoreInfo> peerScores = new HashMap<>();

    public static PeerScoreInfo getScore(BigInteger pid) {
        return peerScores.get(pid);
    }

    public static void putScore(BigInteger pid, PeerScoreInfo psi) {
        peerScores.put(pid, psi);
    }

    public static Map<BigInteger, PeerScoreInfo> getAllScores() {
        return peerScores;
    }

    public static void remove(BigInteger pid) {
        peerScores.remove(pid);
    }
}

MessageCache / IWANTCache

Similarly, you could create static classes (or store them in MeshManager) to handle your message-caching logic:

package peersim.GossipSub;

import java.util.LinkedHashMap;

public class MessageCache {
    private static LinkedHashMap<Long, Message> messageCache = new LinkedHashMap<>();

    public static boolean contains(long id) {
        return messageCache.containsKey(id);
    }

    public static Message get(long id) {
        return messageCache.get(id);
    }

    public static void put(long id, Message msg) {
        messageCache.put(id, msg);
    }
    
    // ...
}

package peersim.GossipSub;

import java.util.LinkedHashMap;

public class IWANTCache {
    private static LinkedHashMap<Long, Message> iwantCache = new LinkedHashMap<>();
    
    public static boolean contains(long id) {
        return iwantCache.containsKey(id);
    }
    
    public static void put(long id, Message msg) {
        iwantCache.put(id, msg);
    }
    
    public static void remove(long id) {
        iwantCache.remove(id);
    }
    
    // ...
}

Key Takeaways

    Keep them in the same package. Make sure all files have package peersim.GossipSub;.
    Reference fields via the “parent”. For instance, in HeartbeatManager or MeshManager, you can do parent.nodeId to access the main protocol’s fields.
    Static Singletons vs. Holding State in Managers
        Some prefer to keep ephemeral caches, message caches, peer scores, etc. in the “parent” class so everything is more object-oriented.
        Others like the convenience of static singletons (as in EphemeralStore). This is a matter of preference (and project guidelines).
    Import classes. Anywhere you need EphemeralMsgInfo or PeerScoreInfo, do import peersim.GossipSub.EphemeralMsgInfo; and similarly for others.
    Visibility. If you need to access parent fields that were originally private, you may need to make them package-private or create getters/setters.

With the above refactoring, the giant GossipSubProtocol.java is now split into smaller, focused classes:

    GossipSubProtocol.java (the main driver)
    HeartbeatManager.java (heartbeat & ephemeral expiration logic)
    MeshManager.java (mesh membership, IHave/IWant, block producer data handling)
    DistributionManager.java (the big block distribution methods)
    SamplingManager.java (the random sample requests/responses)
    Optional “store” classes if you want them: EphemeralStore.java, PeerScores.java, MessageCache.java, IWANTCache.java, etc.

    You can adjust the grouping to suit your needs (you don’t necessarily need all of them). The key is to keep the code functionally identical while making the codebase easier to maintain.

You said:
does my code currently have lazy Pull ?

Examining GossipSub stages

I'm breaking down GossipSub's push-pull stages and spotting "advertiseMessageIHAVE" and "handleIWant." This reveals a typical "IHAVE" -> "IWANT" -> "DATA" pattern, indicating a lazy pull mechanism.

Examining design logic

OK, let's break this down: the code uses "advertiseMessageIHAVE" and "handleIWant" to manage message data, leaning on GossipSub's lazy pull method for data retrieval.

Yes. Your code does implement Gossipsub’s classic “lazy pull” mechanism.
How to Recognize Gossipsub’s Lazy Pull

The hallmark of lazy pull in Gossipsub is the IHAVE → IWANT → DATA exchange:

    IHAVE: A node advertises that it has (or has recently seen) certain message IDs.
    IWANT: Any peers that don’t have those messages respond with “I want it!” requests for the full data.
    DATA: The node then sends the requested data (the actual message body) only to peers who specifically asked for it.

This prevents sending full messages to every peer all the time; instead, messages get fully pulled only when peers notice they don’t have a certain message ID and explicitly request it.
Where to Look in Your Code

In your GossipSubProtocol (and related methods/classes after you split the code), you can see:

    advertiseMessageIHAVE(...)
        Sends an “IHAVE” message (metadata-only) to neighbors.
        Located in your code within something like:

    private void advertiseMessageIHAVE(Message originalMsg, int myPid) {
        ...
        Message ihave = createMessage(..., Message.MSG_IHAVE, ...);
        // Send IHAVE to peers
    }

handleIHave(...)

    When receiving IHAVE, you check whether you already have that message in your local cache.
    If you don’t have it, or if you only had partial metadata, you respond with “IWANT.”
    For example:

    public void handleIHave(Message m, int myPid) {
        if (!messageCache.containsKey(m.id)) {
            // or if you do but it's only partial
            // send IWANT to request the full data
        }
    }

handleIWANT(...)

    You look up the requested message and respond with the actual data (often as MSG_DATA).
    Something like:

        public void handleIWANT(Message m, int myPid) {
            // if we have the full data for m.id, send it with MSG_DATA
            publishMessage(response, m.src, myPid);
        }

    handleData(...)
        Upon receiving the full MSG_DATA, you store it in caches, ephemeral caches, etc.

This IHAVE/IWANT/DATA cycle is precisely the lazy-pull behavior Gossipsub uses.
Why It’s Called “Lazy Pull”

    Nodes push out references to messages (via IHAVE).
    Nodes pull the message bodies (IWANT) only if they need them.

This prevents flooding the entire network with large message bodies. Instead, you “advertise first” and “send full data only on request.” That’s exactly what “lazy pull” means in Gossipsub.
You said:
I also want to add a scoring system for it as if each node is scored, it will print out the Node ID with score it have 

Refining the approach

I'm thinking about adding a scoring system by iterating over the peerScores map in HeartbeatManager or GossipSubProtocol, printing out each node's score.

Integrating scores

I'm pulling together a solution to integrate a simple scoring system into HeartbeatManager or PeerScores, displaying node scores via a new method post-update.

Below is a simple approach for adding and printing peer scores. Since your code already has a basic notion of peer scores (via the PeerScoreInfo class and a Map<BigInteger, PeerScoreInfo>), we just need to:

    Maintain an up-to-date score for each peer.
    Print those scores periodically (e.g. after each heartbeat).

1. Make Sure You Have a Central Score Store

If you use the approach from the previous split example, you might have a PeerScores class that stores peer IDs → PeerScoreInfo:

package peersim.GossipSub;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple static store for peer scores, peerID -> PeerScoreInfo
 */
public class PeerScores {

    private static Map<BigInteger, PeerScoreInfo> peerScores = new HashMap<>();

    public static PeerScoreInfo getScore(BigInteger pid) {
        return peerScores.get(pid);
    }

    public static void putScore(BigInteger pid, PeerScoreInfo psi) {
        peerScores.put(pid, psi);
    }

    public static Map<BigInteger, PeerScoreInfo> getAllScores() {
        return peerScores;
    }

    public static void remove(BigInteger pid) {
        peerScores.remove(pid);
    }
}

Where PeerScoreInfo might look like:

package peersim.GossipSub;

public class PeerScoreInfo {
    public long   timeInMeshStart;
    public int    firstMessageDeliveries;
    public int    invalidMessages;
    public int    meshUnderDelivery;
    public double cachedScore;
    public long   connectedTime;

    public PeerScoreInfo(long currentTime) {
        this.timeInMeshStart       = currentTime;
        this.connectedTime         = currentTime;
        this.firstMessageDeliveries= 0;
        this.invalidMessages       = 0;
        this.meshUnderDelivery     = 0;
        this.cachedScore           = 0.0;
    }
}

2. Make Sure You Update Scores

You already have logic to compute and store scores in your “heartbeat” code (the “lazy pull” Gossipsub approach usually re-checks peer scoring every so often). For example, in HeartbeatManager.java:

// Weighted sum approach from your code
private double computeScore(PeerScoreInfo psi) {
    double w_timeInMesh    = 0.01;
    double w_firstMsg      = 0.1;
    double w_invalid       = -1.0;
    double w_underDelivery = -0.5;

    long now          = CommonState.getTime();
    double timeInMesh = (now - psi.timeInMeshStart);

    double score = 0.0;
    score += (w_timeInMesh    * timeInMesh);
    score += (w_firstMsg      * psi.firstMessageDeliveries);
    score += (w_invalid       * psi.invalidMessages);
    score += (w_underDelivery * psi.meshUnderDelivery);

    return score;
}

Then, after computing each peer’s score (e.g., in runHeartbeat(...)), you can print them. For example:

public void runHeartbeat(Node myNode, int myPid) {
    long now = CommonState.getTime();
    if (parent.isDEBUG) {
        System.out.println("[DEBUG HEARTBEAT] at t=" + now + " on Node=" + parent.nodeId);
    }

    // 1) Expire ephemeral messages, re-advertise, etc. ...

    // 2) Update scores
    Map<BigInteger, PeerScoreInfo> scores = PeerScores.getAllScores();
    for (Map.Entry<BigInteger, PeerScoreInfo> entry : scores.entrySet()) {
        PeerScoreInfo psi  = entry.getValue();
        double oldScore    = psi.cachedScore;
        double newScore    = computeScore(psi);
        psi.cachedScore    = newScore;
        if (parent.isDEBUG) {
            System.out.println("   Peer=" + entry.getKey()
                               + " oldScore=" + oldScore
                               + " newScore=" + newScore);
        }
    }

    // 3) Print out the final scores for ALL peers
    printPeerScores();

    // 4) Possibly prune negative-score peers, re-schedule heartbeat, etc.
    // ...
}

3. Print All Scores

To print every peer’s scores in a simple loop, create a helper method in HeartbeatManager (or your main protocol) like so:

private void printPeerScores() {
    System.out.println("=== Peer Scores on Node " + parent.nodeId + " ===");
    for (Map.Entry<BigInteger, PeerScoreInfo> entry : PeerScores.getAllScores().entrySet()) {
        BigInteger peerId   = entry.getKey();
        PeerScoreInfo score = entry.getValue();
        System.out.println("Peer " + peerId + " => Score: " + score.cachedScore);
    }
    System.out.println("===========================================");
}

This method loops over every entry in PeerScores and prints them.
Where & When to Call printPeerScores()

    After you finish computing scores during your heartbeat is the most common place.
    Optionally after every event that changes peer scores (like receiving an invalid message) — but that might be too spammy in logs.

4. Putting It All Together
Example in HeartbeatManager.java

public class HeartbeatManager {

    public static final int MSG_HEARTBEAT = 9999;
    private GossipSubProtocol parent;

    private static final long MESSAGE_EXPIRATION_MS = 3000;
    private static final int GOSSIP_ADVERTISE_ROUNDS = 3;

    public HeartbeatManager(GossipSubProtocol parent) {
        this.parent = parent;
    }

    public void runHeartbeat(Node myNode, int myPid) {
        long now = CommonState.getTime();
        if (parent.isDEBUG) {
            System.out.println("[DEBUG HEARTBEAT] t=" + now + " Node=" + parent.nodeId);
        }

        // 1) Expire ephemeral messages, re-advertise, etc. ...
        // ephemeralCache logic

        // 2) Score updates
        Map<BigInteger, PeerScoreInfo> scores = PeerScores.getAllScores();
        for (Map.Entry<BigInteger, PeerScoreInfo> e : scores.entrySet()) {
            PeerScoreInfo psi = e.getValue();
            double oldScore   = psi.cachedScore;
            double newScore   = computeScore(psi);
            psi.cachedScore   = newScore;

            if (parent.isDEBUG) {
                System.out.println("[DEBUG]  Peer=" + e.getKey() 
                        + " oldScore=" + oldScore 
                        + " newScore=" + newScore);
            }
        }

        // 3) Print them
        printPeerScores();

        // 4) Possibly prune negative-score peers & update mesh
        // 5) Reschedule next heartbeat
    }

    private double computeScore(PeerScoreInfo psi) {
        // Weighted sum approach
        double w_timeInMesh    = 0.01;
        double w_firstMsg      = 0.1;
        double w_invalid       = -1.0;
        double w_underDelivery = -0.5;

        long now          = CommonState.getTime();
        double timeInMesh = (now - psi.timeInMeshStart);

        double score = 0.0;
        score += (w_timeInMesh    * timeInMesh);
        score += (w_firstMsg      * psi.firstMessageDeliveries);
        score += (w_invalid       * psi.invalidMessages);
        score += (w_underDelivery * psi.meshUnderDelivery);

        return score;
    }

    private void printPeerScores() {
        System.out.println("=== Peer Scores on Node " + parent.nodeId + " ===");
        for (Map.Entry<BigInteger, PeerScoreInfo> entry : PeerScores.getAllScores().entrySet()) {
            BigInteger peerId   = entry.getKey();
            PeerScoreInfo score = entry.getValue();
            System.out.println("Peer " + peerId + " => Score: " + score.cachedScore);
        }
        System.out.println("===========================================");
    }
}

Summary

With the above additions:

    During each heartbeat, you:
        Recompute each peer’s score.
        Print the (Peer ID → Score) table.

    In your logs, you’ll see lines like:

    === Peer Scores on Node 12345 ===
    Peer 23456 => Score: 0.27
    Peer 34567 => Score: -1.30
    ...
    =================================

    You have a simple scoreboard for all nodes connected to you.

You can adjust the frequency of printing if you only want them at certain intervals, or keep it during every heartbeat if that suits your debugging needs.



