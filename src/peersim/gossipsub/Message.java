package peersim.gossipsub;

import java.lang.reflect.Type;
import java.math.BigInteger;

/**
 *
 * Message class provide all functionalities to magage the various messages, principally LOOKUP messages (messages from
 * application level sender destinated to another application level).<br>
 *
 * Types Of messages:<br>
 * (application messages)<BR>
 * - MSG_LOOKUP: indicates that the body Object containes information to application level of the recipient<BR>
 * <br>
 * (service internal protocol messages)<br>
 * - MSG_JOINREQUEST: message containing a join request of a node, the message is passed between many pastry nodes accorting to
 * the protocol<br>
 * - MSG_JOINREPLY: according to protocol, the body transport information related to a join reply message <br>
 * - MSG_LSPROBEREQUEST:according to protocol, the body transport information related to a probe request message <br>
 * - MSG_LSPROBEREPLY: not used in the current implementation<br>
 * - MSG_SERVICEPOLL: internal message used to provide cyclic cleaning service of dead nodes<br>
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

    //Responcse to IWANT
    public static final int MSG_DATA = 3;

    public static final int MSG_BLOCK_PROPOSER = 4;


    // ______________________________________________________________________________________________
    /**
     * This Object contains the body of the message, no matter what it contains
     */
    public Object body = null;

    /**
     * ID of the message. this is automatically generated univocally, and should not change
     */
    public long id;

    /**
     * ACK number of the message. This is in the response message.
     */
    public long ackId;

    /**
     * Recipient address of the message
     */
    public BigInteger dest;

    /**
     * Source address of the message: has to be filled at application level
     */
    public BigInteger src;

    /**
     * Available to count the number of hops the message did.
     */
    protected int nrHops = 0;

    protected String messageTopicID;

    // ______________________________________________________________________________________________
    /**
     * Creates an empty message by using default values (message type = MSG_LOOKUP and <code>new String("")</code> value for the
     * body of the message)
     */
    public Message() {
        this(MSG_EMPTY, "");
    }

    /**
     * Create a message with specific type and empty body
     *
     * @param messageType
     *            int type of the message
     */
    public Message(int messageType) {
        this(messageType, "");
    }

    //Used to create the metadata messages
    public Message(long id, int messageType) {
        super(messageType);
        this.id = id;  // Set the id manually
        this.body = "";
//        this.type = messageType;

    }

    /**
     * Creates a message with specific type and body
     *
     * @param messageType
     *            int type of the message
     * @param body
     *            Object body to assign (shallow copy)
     */
    public Message(int messageType, Object body) {
        super(messageType);
        this.id = (ID_GENERATOR++);
        this.body = body;
    }



    // ______________________________________________________________________________________________
    public String toString() {
        String s = "[ID=" + id + "][DEST=" + dest + "]";
        return s + "[Type=" + messageTypetoString() + "] BODY=(...)";
    }

    // ______________________________________________________________________________________________
    public Message copy() {
        Message dolly = new Message(this.id,this.type);
        dolly.type = this.type;
        dolly.src = this.src;
        dolly.dest = this.dest;
        dolly.body = this.body; // deep cloning?
        dolly.messageTopicID = this.messageTopicID;

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
            default:
                return "UNKNOW:" + type;
        }
    }
}

