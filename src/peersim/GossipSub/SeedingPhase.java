package peersim.GossipSub;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.Set;

/**
 * Implements Phase 1 - Seeding: Assigning Validators (VNs) to rows/columns dynamically
 * using a deterministic function (FNODE).
 */
public class SeedingPhase {

    private final int numberOfRows;
    private final int numberOfColumns;
    private final BigInteger HASHSPACE_SIZE = new BigInteger("2").pow(256); // SHA-256 hash space
    private final Random randomGenerator = new Random();

    public SeedingPhase(int rows, int cols) {
        this.numberOfRows = rows;
        this.numberOfColumns = cols;
    }

    /**
     * Maps a node to a dynamic key in the hashspace for row/column assignment.
     * @param nodeID The unique node identifier (peer ID).
     * @param epoch The epoch number.
     * @param slot The slot number.
     * @param R The random value from the previous block header.
     * @return Assigned row/column index.
     * @throws NoSuchAlgorithmException If SHA-256 is unavailable.
     */
    public int FNODE(BigInteger nodeID, int epoch, int slot, BigInteger R) throws NoSuchAlgorithmException {
        String input = nodeID.toString() + epoch + slot + R.toString();
        BigInteger hashValue = hashSHA256(input);

        BigInteger halfHashSpace = HASHSPACE_SIZE.divide(BigInteger.TWO);

        if (hashValue.compareTo(halfHashSpace) < 0) {
            // Lower half → Assign to a row
            return hashValue.mod(BigInteger.valueOf(numberOfRows)).intValue();
        } else {
            // Upper half → Assign to a column
            return numberOfRows + hashValue.mod(BigInteger.valueOf(numberOfColumns)).intValue();
        }
    }

    /**
     * Assigns seed samples to the mapped validators in a best-effort strategy.
     * @param rowColumnIndex The row/column index assigned.
     * @param vns The set of validators (VN IDs) mapped to the row/column.
     * @return A subset of VN IDs that will receive the seed sample.
     */
    public Set<BigInteger> distributeSeedSamples(int rowColumnIndex, Set<BigInteger> vns) {
        int strategy = randomGenerator.nextInt(2); // Randomly pick a strategy

        if (strategy == 0) {
            // Full row to all VNs (high availability, high resource usage)
            return vns;
        } else {
            // Split row into parts (lower resource usage, less availability)
            return vns.stream().limit(vns.size() / 2).collect(java.util.stream.Collectors.toSet());
        }
    }

    /**
     * Computes the SHA-256 hash of a given string.
     * @param input Input string.
     * @return Hashed BigInteger.
     * @throws NoSuchAlgorithmException If SHA-256 is unavailable.
     */
    private BigInteger hashSHA256(String input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return new BigInteger(1, hash);
    }

    //Test
    // public static void main(String[] args) throws NoSuchAlgorithmException {
    //     SeedingPhase seedingPhase = new SeedingPhase(512, 512);
        
    //     BigInteger nodeID = new BigInteger("1234567890987654321");
    //     int epoch = 1;
    //     int slot = 10;
    //     BigInteger R = new BigInteger("987654321"); // Random value from previous block header
    
    //     int rowColAssignment = seedingPhase.FNODE(nodeID, epoch, slot, R);
    //     System.out.println("Node assigned to Row/Column: " + rowColAssignment);
    // }
    
}
