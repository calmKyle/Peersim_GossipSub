package peersim.GossipSub;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Implements Phase 2 - VN Row/Column Consolidation and Sampling.
 */
public class RowColumnConsolidation {

    private final int numberOfRows;
    private final int numberOfColumns;
    private final Random randomGenerator = new Random();
    private final Map<Integer, Set<BigInteger>> rowColumnAssignments = new HashMap<>(); // Stores VN assignments

    public RowColumnConsolidation(int rows, int cols) {
        this.numberOfRows = rows;
        this.numberOfColumns = cols;
    }

    /**
     * VN attempts to retrieve 73 randomly chosen cells.
     * @param vnID The ID of the validator node.
     * @param assignedIndex The row/column index assigned via FNODE.
     * @return List of randomly chosen cell indices for sampling.
     */
    public List<Integer> performRandomSampling(BigInteger vnID, int assignedIndex) {
        List<Integer> sampleCells = new ArrayList<>();
        Set<BigInteger> vnsInRegion = rowColumnAssignments.getOrDefault(assignedIndex, new HashSet<>());

        // Ensure at least 73 unique cell requests
        while (sampleCells.size() < 73) {
            int cellIndex = randomGenerator.nextInt(512); // Assuming 512 cells per row/column
            if (!sampleCells.contains(cellIndex)) {
                sampleCells.add(cellIndex);
            }
        }

        // Fetch cells from assigned row/column VNs using direct UDP communication
        sendSamplingRequests(vnID, vnsInRegion, sampleCells);

        return sampleCells;
    }

    /**
     * VN consolidates missing cells if it received < 50% from the builder.
     * @param vnID The ID of the validator node.
     * @param assignedIndex The row/column index assigned via FNODE.
     * @param receivedCells The set of received cell indices from the builder.
     * @return True if VN successfully reconstructs the row/column.
     */
    public boolean consolidateRowColumn(BigInteger vnID, int assignedIndex, Set<Integer> receivedCells) {
        int totalCells = 512; // Assuming each row/column has 512 cells
        Set<BigInteger> vnsInRegion = rowColumnAssignments.getOrDefault(assignedIndex, new HashSet<>());

        if (receivedCells.size() >= totalCells / 2) {
            return true; // VN has enough cells to reconstruct
        }

        // Request missing cells from VNs in the same region
        Set<Integer> missingCells = new HashSet<>();
        for (int i = 0; i < totalCells; i++) {
            if (!receivedCells.contains(i)) {
                missingCells.add(i);
            }
        }

        sendConsolidationRequests(vnID, vnsInRegion, missingCells);

        return receivedCells.size() + missingCells.size() >= totalCells / 2;
    }

    /**
     * Sends direct requests for cell retrieval during random sampling.
     * @param vnID The requesting VN ID.
     * @param vnsInRegion The set of VNs holding parts of the row/column.
     * @param requestedCells The list of requested cell indices.
     */
    private void sendSamplingRequests(BigInteger vnID, Set<BigInteger> vnsInRegion, List<Integer> requestedCells) {
        for (BigInteger targetVN : vnsInRegion) {
            System.out.println("VN " + vnID + " requesting random sampling cells from VN " + targetVN);
            // Implement UDP-based direct communication for data retrieval
        }
    }

    /**
     * Sends requests for missing cells during row/column consolidation.
     * @param vnID The requesting VN ID.
     * @param vnsInRegion The set of VNs holding parts of the row/column.
     * @param missingCells The list of missing cell indices.
     */
    private void sendConsolidationRequests(BigInteger vnID, Set<BigInteger> vnsInRegion, Set<Integer> missingCells) {
        for (BigInteger targetVN : vnsInRegion) {
            System.out.println("VN " + vnID + " requesting missing cells from VN " + targetVN);
            // Implement UDP-based direct communication for data retrieval
        }
    }

    /**
     * Assigns VNs to row/column regions for tracking.
     * @param assignedIndex The row/column index assigned via FNODE.
     * @param vnID The VN ID to be assigned.
     */
    public void assignVNToRegion(int assignedIndex, BigInteger vnID) {
        rowColumnAssignments.computeIfAbsent(assignedIndex, k -> new HashSet<>()).add(vnID);
    }
}
