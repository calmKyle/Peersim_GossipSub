package peersim.GossipSub;

import peersim.core.CommonState;

public class PeerScoreInfo {
    public long timeInMeshStart; // Time when the peer joined the mesh
    public int firstMessageDeliveries; // Number of times they delivered a message "first"
    public int invalidMessages; // Count of invalid messages received
    public int meshUnderDelivery; // Count of under-delivery events
    public double cachedScore; // Last computed score
    public long connectedTime; // Time when the peer connected

    public PeerScoreInfo(long currentTime) {
        this.timeInMeshStart = currentTime;
        this.connectedTime = currentTime;
        this.firstMessageDeliveries = 0;
        this.invalidMessages = 0;
        this.meshUnderDelivery = 0;
        this.cachedScore = 0.0;
    }

}
