package peersim.GossipSub.QuickTest;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;

import peersim.GossipSub.PANDAS.SignatureValidator;
import peersim.GossipSub.Block;

public class BlockTest {
    public static void main(String[] args) {
        try {
            // Generate a key pair for testing
            KeyPair keyPair = SignatureValidator.generateKeyPair();

            // Create a block using the validator's key
            Block block = new Block(4, 4, keyPair);

            // Check if the block's signature is valid
            boolean isValid = SignatureValidator.verifySignature(
                String.valueOf(block.blockID).getBytes(),
                block.getSignature(),
                block.getValidatorPublicKey()
            );

            System.out.println("Block Signature Valid: " + isValid);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
