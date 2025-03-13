package peersim.GossipSub;

import peersim.core.Node;
import java.util.ArrayList;
import java.util.Objects;

public class Topic {
    public String topicID;

    public ArrayList<Node> topicMembers; // List containing all nodes in this topic

    public Topic(String topicID) {
        this.topicID = topicID;
        this.topicMembers = new ArrayList<>();
    }

    public void addMember(Node n) {
        topicMembers.add(n);
    }

    public void removeMember(Node n) {
        topicMembers.remove(n);
    }

    // -------------------------------------------------
    // Overriding equals(...) and hashCode(...) so that
    // two Topics with the same topicID are treated as
    // the same for contains(...) etc.
    // -------------------------------------------------
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Topic))
            return false;
        Topic other = (Topic) o;
        return Objects.equals(this.topicID, other.topicID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicID);
    }
}
