package peersim.GossipSub.PANDAS;

import peersim.core.Node;
import java.util.List;

public interface ForkChoiceStrategy {
    Node chooseFork(List<Node> candidateChains, Node currentState);
}
