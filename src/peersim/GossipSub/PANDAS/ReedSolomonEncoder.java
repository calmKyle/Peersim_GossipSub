// package peersim.GossipSub.PANDAS;

// import java.util.Arrays;
// import com.backblaze.erasure.ReedSolomon;

// public class ReedSolomonEncoder {
// private final ReedSolomon rs;

// public ReedSolomonEncoder() {
// this.rs = ReedSolomon.create(256, 256); // 256 data, 256 parity cells
// }

// public byte[][] encode(byte[][] rowData) {
// byte[][] extendedData = new byte[512][512];

// // Copy original 256x256 data
// for (int i = 0; i < 256; i++) {
// System.arraycopy(rowData[i], 0, extendedData[i], 0, 256);
// }

// // Generate Reed-Solomon parity data
// rs.encodeParity(extendedData, 0, 512);

// return extendedData;
// }
// }
