package peersim.GossipSub;
//*************************Same Class as Kademlia******
import java.math.BigInteger;

/**
 * This class represent a timeout event.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class Timeout extends SimpleEvent {

    /**
     * Message Type: PING (used to verify that a node is still alive)
     */
    public static final int TIMEOUT = 200;

    /**
     * The node wich failed to response
     */
    public BigInteger node;

    /**
     * The id of the message sent to the node
     */
    public long msgID;

    public int type; //0 for seeding part messages timeout and 1 for sample messages timeout

//    /**
//     * The id of the operation in wich the message has been sent
//     */
//    public long opID;

    // ______________________________________________________________________________________________
    /**
     * Creates an empty message by using default values (message type = MSG_LOOKUP and <code>new String("")</code> value for the
     * body of the message)
     */
    public Timeout(int type,BigInteger node, long msgID) {
        super(TIMEOUT);

        this.type = type;
        this.node = node;
        this.msgID = msgID;
//        this.opID = opID;
    }
}

//Messes: 1 for message added partNo
//2 added type in timoeut