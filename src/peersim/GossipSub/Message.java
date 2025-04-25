package peersim.GossipSub;

import java.lang.reflect.Type;
import java.math.BigInteger;

/**
 *
 * Message class provide all functionalities to magage the various messages,
 * principally LOOKUP messages (messages from
 * application level sender destinated to another application level).<br>
 *
 * Types Of messages:<br>
 * (application messages)<BR>
 * - MSG_LOOKUP: indicates that the body Object containes information to
 * application level of the recipient<BR>
 * <br>
 * (service internal protocol messages)<br>
 * - MSG_JOINREQUEST: message containing a join request of a node, the message
 * is passed between many pastry nodes accorting to
 * the protocol<br>
 * - MSG_JOINREPLY: according to protocol, the body transport information
 * related to a join reply message <br>
 * - MSG_LSPROBEREQUEST:according to protocol, the body transport information
 * related to a probe request message <br>
 * - MSG_LSPROBEREPLY: not used in the current implementation<br>
 * - MSG_SERVICEPOLL: internal message used to provide cyclic cleaning service
 * of dead nodes<br>
 *
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
// ______________________________________________________________________________________
public class Message extends SimpleEvent {

    /**
     * internal generator for unique message IDs
     */
    private static long ID_GENERATOR = 0;

    /**
     * Message Type: PING (used to verify that a node is still alive)
     */
    public static final int MSG_EMPTY = 0;

    public static final int MSG_IHAVE = 1;
    public static final int MSG_IWANT = 2;

    // Responcse to IWANT
    public static final int MSG_DATA = 3;

    public static final int MSG_BLOCK_PROPOSER = 4;

    public static final int MSG_SAMPLE_DATA_REQUEST = 5;

    public static final int MSG_SAMPLE_DATA_RESPONSE = 6;

    public static final int MSG_START_SAMPLING = 7;

    public static final int MSG_RESET_BANDWIDTH = 8;

    // Gossip Message
    public static final int MSG_HEARTBEAT = 9000;
    public static final int MSG_GRAFT = 9001;
    public static final int MSG_PRUNE = 9002;

    // ______________________________________________________________________________________________
    /**
     * This Object contains the body of the message, no matter what it contains
     */
    public Object body = null;

    /**
     * ID of the message. this is automatically generated univocally, and should not
     * change
     */
    public long id;

    /**
     * ACK number of the message. This is in the response message.
     */
    public long typeID;

    /**
     * Recipient address of the message
     */
    public BigInteger dest;

    /**
     * Source address of the message: has to be filled at application level
     */
    public BigInteger src;

    protected String messageTopicID;

    protected boolean isRow;

    protected int rowOrColumnNumber;

    protected int partNumber;

    public long messageSendingTime; // It is the time stamp at which the message was sent
    // ______________________________________________________________________________________________

    /**
     * Creates an empty message by using default values (message type = MSG_LOOKUP
     * and <code>new String("")</code> value for the
     * body of the message)
     */
    public Message() {
        this(MSG_EMPTY, "", false, -1, -1, -1);
    }

    /**
     * Create a message with specific type and empty body
     *
     * @param messageType
     *                    int type of the message
     */
    public Message(int messageType, boolean isRow, int RoworColNum, int partNo, long typeID) {
        this(messageType, "", isRow, RoworColNum, partNo, typeID);
    }

    // Used to create the metadata messages
    public Message(long id, int messageType, boolean isRow, int rowOrColumnNumber, int partNo, long typeID) {
        super(messageType);
        this.id = id; // Set the id manually
        this.body = "";
        this.isRow = isRow;
        this.rowOrColumnNumber = rowOrColumnNumber;
        this.partNumber = partNo;
        // this.type = messageType;
        this.messageSendingTime = 0;
        this.typeID = typeID;

    }

    /**
     * Creates a message with specific type and body
     *
     * @param messageType
     *                    int type of the message
     * @param body
     *                    Object body to assign (shallow copy)
     */
    public Message(int messageType, Object body, boolean isRow, int rowOrColumnNumber, int partNo, long typeID) {
        super(messageType);
        this.id = (ID_GENERATOR++);
        this.body = body;
        this.isRow = isRow;
        this.rowOrColumnNumber = rowOrColumnNumber;
        this.partNumber = partNo;
        this.typeID = typeID;
    }

    // ______________________________________________________________________________________________
    public String toString() {
        String s = "[ID=" + id + "][DEST=" + dest + "]";
        return s + "[Type=" + messageTypetoString() + "] BODY=(...)";
    }

    // ______________________________________________________________________________________________
    public Message copy() {
        Message dolly = new Message(this.id, this.type, this.isRow, this.rowOrColumnNumber, this.partNumber,
                this.typeID);
        dolly.type = this.type;
        dolly.src = this.src;
        dolly.dest = this.dest;
        dolly.body = this.body; // deep cloning?
        dolly.messageTopicID = this.messageTopicID;
        dolly.messageSendingTime = this.messageSendingTime;
        // dolly.isRow = this.isRow;
        // dolly.rowOrColumnNumber =this.rowOrColumnNumber;

        return dolly;
    }

    // ______________________________________________________________________________________________
    public String messageTypetoString() {
        switch (type) {
            case MSG_EMPTY:
                return "MSG_EMPTY";
            case MSG_IHAVE:
                return "MSG_IHAVE";
            case MSG_IWANT:
                return "MSG_IWANT";
            case MSG_DATA:
                return "MSG_RESPONSE";
            case MSG_BLOCK_PROPOSER:
                return "MSG_BLOCK_PROPOSER";
            case MSG_SAMPLE_DATA_REQUEST:
                return "MSG_SAMPLE_DATA_REQUEST";
            case MSG_SAMPLE_DATA_RESPONSE:
                return "MSG_SAMPLE_DATA_RESPONSE";
            case MSG_START_SAMPLING:
                return "MSG_START_SAMPLING";
            case MSG_RESET_BANDWIDTH:
                return "MSG_RESET_BANDWIDTH";
            case MSG_HEARTBEAT:
                return "MSG_HEARTBEAT";
            case MSG_GRAFT:
                return "MSG_GRAFT";
            case MSG_PRUNE:
                return "MSG_PRUNE";
            default:
                return "UNKNOW:" + type;
        }
    }
}
