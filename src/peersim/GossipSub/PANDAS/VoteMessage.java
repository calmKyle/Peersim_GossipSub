package peersim.GossipSub.PANDAS;

import peersim.gossipsub.Message;
import java.math.BigInteger;

public class VoteMessage extends Message {
    private boolean approval;
    private long blockID;

    public VoteMessage(BigInteger src, BigInteger dest, boolean approval, long blockID) {
        super(Message.MSG_BLOCK_PROPOSER, null, false, 0, 0, 0);
        this.src = src;
        this.dest = dest;
        this.approval = approval;
        this.blockID = blockID;
    }

    public boolean isApproval() {
        return approval;
    }

    public long getBlockID() {
        return blockID;
    }
}
