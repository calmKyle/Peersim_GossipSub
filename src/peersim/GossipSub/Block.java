package peersim.GossipSub;

import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Arrays;

import peersim.GossipSub.PANDAS.SignatureValidator;

public class Block {
    public static int block_id_counter = 0;
    public int blockID;
    public byte[][][] sampleMatrix;
    public int rows;
    public int columns;
    public static final int ELEMENT_SIZE = 512; // Size of each element in bytes
    public long timestamp;

    // Validation and Signature
    private PublicKey validatorPublicKey; // Store the validator's public key
    private byte[] signature; // Digital signature of the block

    // public Block(int r, int c, KeyPair validatorKeyPair) {
    // this.blockID = block_id_counter++;
    // this.rows = r;
    // this.columns = c;
    // this.sampleMatrix = new byte[rows][columns][ELEMENT_SIZE];
    // this.timestamp = System.currentTimeMillis(); // check the current timestamp
    // initialiseDataMatrix();

    // this.validatorPublicKey = validatorKeyPair.getPublic();

    // // Generate a digital signature for this block
    // try {
    // this.signature =
    // SignatureValidator.signData(String.valueOf(blockID).getBytes(),
    // validatorKeyPair.getPrivate());
    // } catch (Exception e) {
    // e.printStackTrace();
    // this.signature = new byte[0]; // Default empty signature
    // }
    // }

    public Block(int r, int c, KeyPair validatorKeyPair) {
        this.blockID = block_id_counter++;
        this.rows = r;
        this.columns = c;
        this.sampleMatrix = new byte[rows][columns][ELEMENT_SIZE];
        this.timestamp = System.currentTimeMillis();
        initialiseDataMatrix();

        if (validatorKeyPair != null) {
            this.validatorPublicKey = validatorKeyPair.getPublic();
            try {
                this.signature = SignatureValidator.signData(String.valueOf(blockID).getBytes(),
                        validatorKeyPair.getPrivate());
            } catch (Exception e) {
                e.printStackTrace();
                this.signature = new byte[0]; // Default empty signature
            }
        } else {
            this.validatorPublicKey = null;
            this.signature = new byte[0]; // No signature in test mode
        }
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

    public long getTimestamp() {
        return timestamp;
    }

    public PublicKey getValidatorPublicKey() {
        return validatorPublicKey;
    }

    public byte[] getSignature() {
        return signature;
    }
}
