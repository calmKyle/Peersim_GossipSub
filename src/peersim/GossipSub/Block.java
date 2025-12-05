package peersim.GossipSub;

import java.util.Arrays;

public class Block {
    public static int block_id_counter = 0;
    public int blockID;
    public byte[][][] sampleMatrix;
    public int rows;
    public int columns;
    public static final int ELEMENT_SIZE = 512; // Size of each element in bytes

    public Block(int r, int c) {
        System.out.printf("BLOCK #%d: \n", block_id_counter++);
        this.blockID = block_id_counter++;
        this.rows = r;
        this.columns = c;
        this.sampleMatrix = new byte[rows][columns][ELEMENT_SIZE];
        initialiseDataMatrix();
    }

    public void initialiseDataMatrix() {
        byte data = 1;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                Arrays.fill(sampleMatrix[i][j], data++);
            }
        }
    }

    public byte[][] getRowData(int r) { // Sends the rth row
        return sampleMatrix[r];
    }

    public byte[][] getColumnData(int c) { // Sends the cth column
        byte[][] column = new byte[rows][ELEMENT_SIZE];
        for (int i = 0; i < rows; i++) {
            column[i] = sampleMatrix[i][c];
        }
        return column;
    }

    public byte[] getSample(int r, int c) {
        return sampleMatrix[r][c];
    }
}
