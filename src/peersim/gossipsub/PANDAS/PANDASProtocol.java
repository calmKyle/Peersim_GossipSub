package peersim.gossipsub;

import peersim.core.*;
import peersim.config.*;
import peersim.edsim.*;

public class PANDASProtocol implements EDProtocol {

  private static final String PAR_BUILDER = "builder";
  private SeedMessageHandler seedHandler;

  public PANDASProtocol(String prefix) {
    // Initialization from config file
    this.seedHandler = new SeedMessageHandler(prefix);
  }

  @Override
  public void processEvent(Node node, int pid, Object event) {
    if (event instanceof SeedMessage) {
      seedHandler.handleSeedMessage(node, (SeedMessage) event);
    } else if (event instanceof SampleMessage) {
      handleSampleMessage(node, (SampleMessage) event);
    } else if (event instanceof VoteMessage) {
      handleVoteMessage(node, (VoteMessage) event);
    } else if (event instanceof ConsensusMessage) {
      handleConsensusMessage(node, (ConsensusMessage) event);
    }
  }

  private void handleSeedMessage(Node node, SeedMessage msg) {
    // Handle seed message for row/column consolidation
    seedHandler.handleSeedMessage(node, msg);
    System.out.println("Seed message received by " + node.getID());
  }

  private void handleSampleMessage(Node node, SampleMessage msg) {
    // Handle sample message for row/column consolidation
  }

  private void handleVoteMessage(Node node, VoteMessage msg) {
    // Handle vote message for row/column consolidation
  }

  private void handleConsensusMessage(Node node, ConsensusMessage msg) {
    // Handle consensus message for row/column consolidation
  }

  @Override
  public Object clone() {
    return new PANDASProtocol();
  }
}

// Messages that would be used in the PANDAS protocol
class SeedMessage {
  final Node sender;
  final Object data; // This would represent the data chunk sent

  public SeedMessage(Node sender, Object data) {
    this.sender = sender;
    this.data = data;
  }
}

class SampleMessage {
  final Node sender;
  final Object sample;

  public SampleMessage(Node sender, Object sample) {
    this.sender = sender;
    this.sample = sample;
  }
}
