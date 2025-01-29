package peersim.gossipsub.PANDAS;

import peersim.core.Node;
import java.util.List;
import java.util.Comparator;

public class LMDGHOST implements ForkChoiceStrategy {
    @Override
    public Node chooseFork(List<Node> candidateChains, Node currentState) {
        // LMD GHOST fork choice rule selects the highest-weighted chain
        return candidateChains.stream()
                .max(Comparator.comparingInt(this::getWeight)) // ✅ Use a method to get weight
                .orElse(currentState);
    }

    /**
     * Simulates weight calculation for LMD GHOST.
     * In real implementation, you must replace this with actual fork weight logic.
     */
    private int getWeight(Node node) {
        // TODO: Replace with real logic to determine weight of a node
        return (int) node.getID() % 100; // Placeholder: using node ID as weight
    }
}
