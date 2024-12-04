package peersim.gossipsub;

import peersim.core.Node;

import java.util.ArrayList;

public class Topic
{
    public String topicID;

    public ArrayList<Node> topicMembers; //List containing all the nodes in a particular topic

    public Topic(String topicID)
    {
        this.topicID = topicID;
        topicMembers = new ArrayList<>();
    }

    public void addMember(Node n)
    {
        topicMembers.add(n);
    }

}
