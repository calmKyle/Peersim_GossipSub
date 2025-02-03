package peersim.GossipSub.PANDAS;

import peersim.GossipSub.Block;
import peersim.core.Node;
import java.util.List;
import java.util.Comparator;

public class TightForkChoice implements ForkChoiceStrategy {

    @Override
    public Node chooseFork(List<Node> candidateChains, Node currentState) {
        return candidateChains.stream()
                .filter(this::isValid)
                .max(Comparator.comparingLong(this::getTimestamp))
                .orElse(currentState);
    }

    private boolean isValid(Node node) {
        if (!(node instanceof Block)) return false;
        Block block = (Block) node;

        try {
            return SignatureValidator.verifySignature(
                String.valueOf(block.blockID).getBytes(),  // The block ID as the signed data
                block.getSignature(),  // The stored signature
                block.getValidatorPublicKey()  // Validator's public key
            );
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private long getTimestamp(Node node) {
        if (!(node instanceof Block)) return 0;
        return ((Block) node).getTimestamp();
    }
}
