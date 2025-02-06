package peersim.GossipSub;

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

    public void removeMember(Node n)
    {
        int i=0;
        for(Node node:topicMembers)
        {
            if(node.equals(n))
            {
                topicMembers.remove(i);
                return;
            }
            i++;
        }
    }

}
