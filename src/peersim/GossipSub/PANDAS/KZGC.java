package peersim.GossipSub.PANDAS;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class KZGC {
    public static byte[] generateCommitment(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        return Arrays.copyOf(sha256.digest(data), 48); // Take first 48 bytes
    }

    public static boolean verifyCommitment(byte[] data, byte[] commitment) throws NoSuchAlgorithmException {
        return Arrays.equals(generateCommitment(data), commitment);
    }
}
