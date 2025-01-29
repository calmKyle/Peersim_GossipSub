package peersim.gossipsub.PANDAS;

import peersim.core.Node;
import peersim.gossipsub.Message;

import java.math.BigInteger;

public class SeedMessage extends Message {
    private int blockID;
    private int rows;
    private int columns;
    private Object data;
    private Node sender;

    public SeedMessage(BigInteger src, BigInteger dest, int blockID, int rows, int columns, Object data) {
        super(Message.MSG_BLOCK_PROPOSER, null, false, rows, columns, 0);
        this.src = src;
        this.dest = dest;
        this.blockID = blockID;
        this.rows = rows;
        this.columns = columns;
        this.data = data;
    }

    public SeedMessage(Node sender, Object data, int blockID, int rows, int columns) {
        super(Message.MSG_BLOCK_PROPOSER, null, false, rows, columns, 0); 
        this.sender = sender;
        this.data = data;
        this.blockID = blockID;
        this.rows = rows;
        this.columns = columns;
    }

    public int getBlockID() {
        return blockID;
    }

    public int getRows() {
        return rows;
    }

    public int getColumns() {
        return columns;
    }

    public Object getData() {
        return data;
    }

    public Node getSender() {
        return sender;
    }
}
