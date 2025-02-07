package peersim.GossipSub.PANDAS;

import peersim.config.Configuration;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DeterministicAssignment {
    /**
     * Determines the nodes assigned to a row/column using a deterministic mapping function.
     */
    public static List<BigInteger> getAssignedNodes(int rowOrColNum, boolean isRow, long epoch, long slot, BigInteger randomValueR) {
        List<BigInteger> assignedVNs = new ArrayList<>();
        int numValidators = Configuration.getInt("NUM_VALIDATOR_NODES", 8192);

        // Use a deterministic function to select assigned validators
        for (int i = 0; i < numValidators; i++) {
            BigInteger nodeId = calculateNodeAssignment(i, rowOrColNum, isRow, epoch, slot, randomValueR);
            assignedVNs.add(nodeId);
        }

        return assignedVNs;
    }

    /**
     * Deterministically assigns a validator to a row/column.
     */
    private static BigInteger calculateNodeAssignment(int index, int rowOrColNum, boolean isRow, long epoch, long slot, BigInteger randomValueR) {
        BigInteger hash = HashFunction.hash(index + "-" + rowOrColNum + "-" + (isRow ? "row" : "col") + "-" + epoch + "-" + slot + "-" + randomValueR);
        return hash.mod(BigInteger.valueOf(Configuration.getInt("NUM_VALIDATOR_NODES", 8192)));
    }
}
