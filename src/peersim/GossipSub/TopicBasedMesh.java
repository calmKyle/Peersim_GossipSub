package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import java.math.BigInteger;
import java.util.*;

public class TopicBasedMesh {
    private static final String PAR_PROT = "protocol";
    private final int gossipProtocolID;

    public TopicBasedMesh(String prefix) {
        this.gossipProtocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    public void createTopicMesh() {
        System.out.println("Initializing GossipSub mesh for topics…");

        for (Topic topic : CustomDistribution.topics.values()) {
            if (topic.topicMembers.size() <= 1) continue;     // trivial topic

            /* one immutable snapshot for this topic */
            Set<BigInteger> snapshot = new HashSet<>();
            for (Node n : topic.topicMembers) {
                BigInteger id = ((GossipSubProtocol) n.getProtocol(gossipProtocolID)).nodeId;
                snapshot.add(id);
            }

            /* every node stores its *own* copy */
            for (Node n : topic.topicMembers) {
                GossipSubProtocol g = (GossipSubProtocol) n.getProtocol(gossipProtocolID);
                g.setTopicMembersList(topic.topicID, new HashSet<>(snapshot));
                g.meshPeersByTopic.computeIfAbsent(topic.topicID, k -> new HashSet<>());
            }

            /* first pass ─ naïve greedy connects                                              */
            List<Node> shuffled = new ArrayList<>(topic.topicMembers);
            Collections.shuffle(shuffled, CommonState.r);
            for (Node n : shuffled) connectPeers((GossipSubProtocol) n.getProtocol(gossipProtocolID),
                    shuffled, topic.topicID);

            /* second pass ─ make sure everyone has ≥ D_LO peers immediately                  */
            for (Node n : shuffled) {
                GossipSubProtocol g = (GossipSubProtocol) n.getProtocol(gossipProtocolID);
                int missing = g.D_LOW - g.meshPeersByTopic.get(topic.topicID).size();
                if (missing > 0) addMorePeers(g, topic.topicID, missing);
            }
        }
    }

    private void connectPeers(GossipSubProtocol g, List<Node> topicNodes, String tid) {
        List<Node> peers = new ArrayList<>(topicNodes);
        Collections.shuffle(peers, CommonState.r);

        for (Node p : peers) {
            if (g.meshPeersByTopic.get(tid).size() >= g.D) break;

            GossipSubProtocol pg = (GossipSubProtocol) p.getProtocol(gossipProtocolID);
            if (pg.meshPeersByTopic.get(tid).size() >= pg.D)  continue;
            if (g.nodeId.equals(pg.nodeId))                   continue;

            g.meshPeersByTopic.get(tid).add(pg.nodeId);
            pg.meshPeersByTopic.get(tid).add(g.nodeId);
        }
    }

    /** Opportunistically add <code>count</code> more peers to <code>g</code>. */
    private void addMorePeers(GossipSubProtocol g, String tid, int count) {
        List<Node> pool = new ArrayList<>(CustomDistribution.topics.get(tid).topicMembers);
        Collections.shuffle(pool, CommonState.r);

        for (Node p : pool) {
            if (count == 0) break;
            GossipSubProtocol pg = (GossipSubProtocol) p.getProtocol(gossipProtocolID);
            if (pg.nodeId.equals(g.nodeId))                   continue;
            if (g.meshPeersByTopic.get(tid).contains(pg.nodeId)) continue;
            if (pg.meshPeersByTopic.get(tid).size() >= pg.D)  continue;

            g.meshPeersByTopic.get(tid).add(pg.nodeId);
            pg.meshPeersByTopic.get(tid).add(g.nodeId);
            --count;
        }
    }
}
