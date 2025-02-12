package peersim.GossipSub.PANDAS;

import java.util.Map;
import java.util.Random;

public class DataAvailabilitySampling {
    public static boolean performDASCheck(Map<int[], MatrixCell> matrix) {
        int totalCells = 512 * 512;
        int sampledCells = totalCells / 4; // 25% sampling

        Random rand = new Random();
        for (int i = 0; i < sampledCells; i++) {
            int row = rand.nextInt(512);
            int col = rand.nextInt(512);
            MatrixCell cell = matrix.get(new int[] { row, col });

            try {
                if (cell == null || !KZGC.verifyCommitment(cell.data, cell.kzgc)) {
                    System.out.println("DAS Check Failed: Missing or invalid cell at (" + row + "," + col + ")");
                    return false;
                }
            } catch (Exception e) {
                System.out.println(
                        "DAS Check Failed: Error verifying cell at (" + row + "," + col + "): " + e.getMessage());
            }

        }
        System.out.println("DAS Check Passed: Block is available.");
        return true;
    }
}
