package peersim.gossipsub.PANDAS;

import peersim.core.Node;
import java.util.List;
import java.util.Comparator;

public class TightForkChoice implements ForkChoiceStrategy {
    @Override
    public Node chooseFork(List<Node> candidateChains, Node currentState) {
        // Prioritize the most recent block that follows protocol rules
        return candidateChains.stream()
                .filter(this::isValid) // ✅ Calls a custom validation method
                .max(Comparator.comparingLong(this::getTimestamp)) // ✅ Uses a custom method to get timestamps
                .orElse(currentState);
    }

    private boolean isValid(Node node) {
        // TODO: Implement protocol-specific validation (e.g., checking signatures, block validity, etc.)
        return true; // Placeholder for now
    }

    private long getTimestamp(Node node) {
        // TODO: Replace with actual logic to get the node's timestamp
        return node.getID(); // Using node ID as a placeholder (update this!)
    }
}
