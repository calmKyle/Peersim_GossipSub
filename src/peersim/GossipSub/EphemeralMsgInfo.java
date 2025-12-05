package peersim.GossipSub;

import peersim.core.CommonState;

public class EphemeralMsgInfo {
    public Message message;
    public long arrivalTime; // When we first stored the message
    public int advertiseCount; // How many times we've re-advertised (IHAVE)

    public EphemeralMsgInfo(Message m) {
        this.message = m;
        this.arrivalTime = CommonState.getTime();
        this.advertiseCount = 0;
    }
}
