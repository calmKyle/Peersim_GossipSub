package peersim.GossipSub.PANDAS;

import java.math.BigInteger;
import peersim.core.Node;
import peersim.gossipsub.Message;

public class SampleMessage extends Message {
    private int row;
    private int column;
    private int blockID;

    // Existing constructor
    public SampleMessage(BigInteger src, BigInteger dest, int blockID, int row, int column) {
        super(Message.MSG_SAMPLE_DATA_REQUEST, null, false, -1, -1, -1);
        this.src = src;
        this.dest = dest;
        this.blockID = blockID;
        this.row = row;
        this.column = column;
    }

    // NEW CONSTRUCTOR to allow Node as input
    public SampleMessage(Node node, int blockID) {
        super(Message.MSG_SAMPLE_DATA_REQUEST, null, false, -1, -1, -1);
        this.src = BigInteger.valueOf(node.getID());
        this.dest = BigInteger.valueOf(node.getID());
        this.blockID = blockID;
        this.row = 0; // Default row
        this.column = 0; // Default column
    }

    public int getRow() { return row; }
    public int getColumn() { return column; }
    public int getBlockID() { return blockID; }
}
