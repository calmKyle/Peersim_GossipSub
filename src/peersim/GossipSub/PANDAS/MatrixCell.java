package peersim.GossipSub.PANDAS;

public class MatrixCell {
    public byte[] data; // 512 bytes of actual data
    public byte[] kzgc; // 48-byte cryptographic commitment

    public MatrixCell(byte[] data, byte[] kzgc) {
        this.data = data;
        this.kzgc = kzgc;
    }
}
