package peersim.GossipSub;

import java.lang.reflect.Type;
import java.math.BigInteger;
import peersim.gossipsub.SimpleEvent;

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

    // Internal generator for unique message IDs
    private static long ID_GENERATOR = 0;

    // Message Types
    public static final int MSG_EMPTY = 0;
    public static final int MSG_IHAVE = 1;
    public static final int MSG_IWANT = 2;
    public static final int MSG_DATA = 3;
    public static final int MSG_BLOCK_PROPOSER = 4;
    public static final int MSG_SAMPLE_DATA_REQUEST = 5;
    public static final int MSG_SAMPLE_DATA_RESPONSE = 6;
    public static final int MSG_START_SAMPLING = 7;
    public static final int MSG_RESET_BANDWIDTH = 8; // Restored for bandwidth control

    // Message Fields
    public Object body;
    public long id;
    public long ackId;
    public BigInteger dest;
    public BigInteger src;

    protected String messageTopicID;
    protected boolean isRow;
    protected int rowOrColumnNumber;
    protected int partNumber;
    public long messageSendingTime; // Timestamp when the message was sent

    /**
     * Default Constructor: Creates an empty message.
     */
    public Message() {
        this(MSG_EMPTY, "", false, -1, -1, -1);
    }

    /**
     * Constructor for messages with a type and empty body.
     */
    public Message(int messageType, boolean isRow, int rowOrColNum, int partNo, long ackId) {
        this(messageType, "", isRow, rowOrColNum, partNo, ackId);
    }

    /**
     * Constructor for metadata messages.
     */
    public Message(long id, int messageType, boolean isRow, int rowOrColNum, int partNo, long ackId) {
        super(messageType);
        this.id = id;
        this.body = "";
        this.isRow = isRow;
        this.rowOrColumnNumber = rowOrColNum;
        this.partNumber = partNo;
        this.ackId = ackId;
        this.messageSendingTime = 0;
    }

    /**
     * Constructor for messages with a specific type and body.
     */
    public Message(int messageType, Object body, boolean isRow, int rowOrColNum, int partNo, long ackId) {
        super(messageType);
        this.id = ID_GENERATOR++;
        this.body = deepCopyBody(body);
        this.isRow = isRow;
        this.rowOrColumnNumber = rowOrColNum;
        this.partNumber = partNo;
        this.ackId = ackId;
    }

    /**
     * Creates a deep copy of the message body to prevent unintentional
     * modifications.
     */
    // private Object deepCopyBody(Object body) {
    // if (body instanceof String) {
    // return new String((String) body);
    // } else if (body instanceof byte[][]) {
    // byte[][] original = (byte[][]) body;
    // byte[][] copy = new byte[original.length][];
    // for (int i = 0; i < original.length; i++) {
    // copy[i] = original[i].clone();
    // }
    // return copy;
    // } else if (body instanceof byte[][][]) {
    // byte[][][] original = (byte[][][]) body;
    // byte[][][] copy = new byte[original.length][][];
    // for (int i = 0; i < original.length; i++) {
    // copy[i] = new byte[original[i].length][];
    // for (int j = 0; j < original[i].length; j++) {
    // copy[i][j] = original[i][j].clone();
    // }
    // }
    // return copy;
    // }
    // return body; // Default case: return as-is for other object types
    // }
    private Object deepCopyBody(Object body) {
        if (body instanceof byte[][]) {
            byte[][] original = (byte[][]) body;
            byte[][] copy = new byte[original.length][];
            for (int i = 0; i < original.length; i++) {
                if (original[i] != null) {
                    copy[i] = original[i].clone();
                } else {
                    copy[i] = new byte[0];
                }
            }
            return copy;
        } else if (body instanceof byte[][][]) {
            byte[][][] original = (byte[][][]) body;
            byte[][][] copy = new byte[original.length][][];
            for (int i = 0; i < original.length; i++) {
                if (original[i] != null) {
                    copy[i] = new byte[original[i].length][];
                    for (int j = 0; j < original[i].length; j++) {
                        if (original[i][j] != null) {
                            copy[i][j] = original[i][j].clone();
                        } else {
                            copy[i][j] = new byte[0];
                        }
                    }
                } else {
                    copy[i] = new byte[0][0];
                }
            }
            return copy;
        }
        return body; // Default case
    }

    /**
     * Returns a string representation of the message.
     */
    @Override
    public String toString() {
        return "[ID=" + id + "][SRC=" + src + "][DEST=" + dest + "][Type=" + messageTypetoString() + "]";
    }

    /**
     * Creates a copy of this message.
     */
    public Message copy() {
        Message clone = new Message(this.id, this.type, this.isRow, this.rowOrColumnNumber, this.partNumber,
                this.ackId);
        clone.src = this.src;
        clone.dest = this.dest;
        clone.body = deepCopyBody(this.body);
        clone.messageTopicID = this.messageTopicID;
        clone.messageSendingTime = this.messageSendingTime;
        return clone;
    }

    /**
     * Converts message type to a human-readable string.
     */
    public String messageTypetoString() {
        switch (type) {
            case MSG_EMPTY:
                return "MSG_EMPTY";
            case MSG_IHAVE:
                return "MSG_IHAVE";
            case MSG_IWANT:
                return "MSG_IWANT";
            case MSG_DATA:
                return "MSG_DATA";
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
            default:
                return "UNKNOWN:" + type;
        }
    }
}
