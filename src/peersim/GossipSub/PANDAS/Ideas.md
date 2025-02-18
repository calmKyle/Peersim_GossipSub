# GossipSubProtocol.java
## Adding healMesh for each cycle
```java
public void healMesh() {
    for (String topicID : subscribedTopics.stream().map(t -> t.topicID).collect(Collectors.toList())) {
        if (!localMesh.containsKey(topicID)) continue;

        Set<BigInteger> peers = localMesh.get(topicID);
        if (peers.size() < minDegree) {
            // Add more peers to restore connectivity
            int neededPeers = minDegree - peers.size();
            TopicBasedMesh meshManager = new TopicBasedMesh(GossipSubProtocol.prefix);
            meshManager.addMorePeers(this, topicID, neededPeers);
            System.out.println("Mesh healed for " + topicID + ", added " + neededPeers + " peers.");
        }
    }
}

```

## Making Adaptive K Copies Distribution 
```java 
private void adaptiveKCopiesDistribution() {
    int defaultK = Configuration.getInt("NUMBER_COPIES_DISTRIBUTED");
    int adjustedK = Math.min(16, defaultK + (int) (NoOfSampleRequestsSent * 0.05)); // Increase K if needed

    System.out.println("Adaptive K copies: " + adjustedK);

    Block b = new Block(Configuration.getInt("NUMBER_OF_ROWS"), Configuration.getInt("NUMBER_OF_COLUMNS"));
    int rowNumber = 0, columnNumber = 0;

    for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
        int cnt = 0, nodeCounter = 0, numberOfCopiesSent = 0;

        for (Node n : topicEntry.getValue().topicMembers) {
            GossipSubProtocol g = (GossipSubProtocol) n.getProtocol(gossipSubId);

            if (cnt < (Configuration.getInt("NUMBER_OF_ROWSCOLS_IN_A_TOPIC") / 2)) {
                if (numberOfCopiesSent >= adjustedK && nodeCounter % 8 != 0) {
                    nodeCounter++;
                    if (nodeCounter % 8 == 0) {
                        numberOfCopiesSent = 0;
                        rowNumber++;
                        cnt++;
                    }
                    continue;
                }
                byte[][] rowToSend = b.getRowData(rowNumber);
                sendBlockData(g, rowToSend, rowNumber, topicEntry.getValue().topicID);
                nodeCounter++;
                numberOfCopiesSent++;
            } else {
                if (numberOfCopiesSent >= adjustedK && nodeCounter % 8 != 0) {
                    nodeCounter++;
                    if (nodeCounter % 8 == 0) {
                        numberOfCopiesSent = 0;
                        columnNumber++;
                        cnt++;
                    }
                    continue;
                }
                byte[][] colToSend = b.getColumnData(columnNumber);
                sendBlockData(g, colToSend, columnNumber, topicEntry.getValue().topicID);
                nodeCounter++;
                numberOfCopiesSent++;
            }
        }
    }
}
```
## Adaptive Retire for failed Sample
```java 
private void retryFailedSamples() {
    int retryLimit = 3;
    for (BigInteger missingNode : sampleRequestFailures.keySet()) {
        int retryCount = sampleRequestFailures.get(missingNode);
        if (retryCount < retryLimit) {
            sampleRequestFailures.put(missingNode, retryCount + 1);
            resendSampleRequest(missingNode);
        } else {
            System.out.println("Max retries reached for node " + missingNode);
        }
    }
}

private void resendSampleRequest(BigInteger nodeId) {
    System.out.println("Retrying sample request to " + nodeId);
    Message retryRequest = createMessage(
            -1, Message.MSG_SAMPLE_DATA_REQUEST, this.nodeId, nodeId, "RetryTopic",
            "", true, 0, -1, 0, -1);
    publishMessage(retryRequest, nodeId, gossipSubId);
}

```
# DynamicMeshControl.java
## Repair broken meshes automatically.
```java
@Override
public boolean execute() {
    for (Map.Entry<String, Topic> topicEntry : CustomDistribution.topics.entrySet()) {
        Topic topic = topicEntry.getValue();
        for (Node node : topic.topicMembers) {
            GossipSubProtocol gsp = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
            
            // Heal broken mesh connections
            if (gsp.localMesh.get(topic.topicID).size() < gsp.minDegree) {
                gsp.healMesh();
            }
        }
    }
    return false;
}
```


# CustomDistribution
- Ensure for each topics have Fnode (16 for example)

```java
private int Fnode = 16;
private void balanceTopics() {
    for (Map.Entry<String, Topic> topicEntry : topics.entrySet()) {
        Topic topic = topicEntry.getValue();
        if (topic.topicMembers.size() < Fnode) {
            int needed = Fnode - topic.topicMembers.size();
            System.out.println("Balancing topic " + topic.topicID + ", adding " + needed + " nodes.");
            assignExtraNodesToTopic(topic, needed);
        }
    }
}

private void assignExtraNodesToTopic(Topic topic, int needed) {
    List<Node> availableNodes = new ArrayList<>(networkNodes.values());
    Collections.shuffle(availableNodes);

    for (Node node : availableNodes) {
        if (needed <= 0) break;
        GossipSubProtocol gossipNode = (GossipSubProtocol) node.getProtocol(gossipProtocolID);
        if (!topic.topicMembers.contains(node)) {
            topic.addMember(node);
            gossipNode.subscribeTopic(topic);
            needed--;
        }
    }
}

```
