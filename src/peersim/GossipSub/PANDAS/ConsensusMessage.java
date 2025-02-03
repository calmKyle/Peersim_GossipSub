package peersim.GossipSub.PANDAS;

import peersim.GossipSub.Message;
import java.math.BigInteger;

public class ConsensusMessage extends Message {
    private long blockID;
    private Object decision;

    public ConsensusMessage(BigInteger src, BigInteger dest, long blockID, Object decision) {
        super(Message.MSG_BLOCK_PROPOSER, null, false, 0, 0, 0);
        this.src = src;
        this.dest = dest;
        this.blockID = blockID;
        this.decision = decision;
    }

    public long getBlockID() {
        return blockID;
    }

    public Object getDecision() {
        return decision;
    }
}
