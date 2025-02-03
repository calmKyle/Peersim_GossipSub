package peersim.GossipSub.PANDAS;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;

/**
 * Class to handle seed messages within the PANDAS protocol.
 */
public class SeedMessageHandler implements EDProtocol {

  public static final String PAR_PROT = "protocol";
  private final int protocolId;
  private final String prefix;  // Store the prefix for cloning

  public SeedMessageHandler(String prefix) {
    this.prefix = prefix;  // Save prefix for cloning
    this.protocolId = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  @Override
  public void processEvent(Node node, int pid, Object event) {
    if (event instanceof SeedMessage) {
      receiveSeed((SeedMessage) event, node);
    }
  }

  /**
   * Handles the reception of a seed message.
   * 
   * @param msg  the received seed message
   * @param node the node that received the message
   */
  private void receiveSeed(SeedMessage msg, Node node) {
    System.out.println("Node " + node.getID() + " received a seed from " + (msg.getSender()).getID());

    if (verifyData(msg.getData())) {
      System.out.println("Data verified successfully at Node " + node.getID());
      forwardData(node, msg.getData());
    } else {
      System.out.println("Data verification failed at Node " + node.getID());
    }
  }

  /**
   * Verifies the data received in the seed message.
   * 
   * @param data the data to verify
   * @return true if the data is correct, false otherwise
   */
  public boolean verifyData(Object data) {
    return true; // Placeholder for actual verification logic
  }

  /**
   * Forwards the data to other nodes or performs further processing.
   * 
   * @param node the current node
   * @param data the data to forward
   */
  private void forwardData(Node node, Object data) {
    System.out.println("Node " + node.getID() + " is forwarding data.");
    EDSimulator.add(100, new SeedMessage(node, data, protocolId, protocolId, protocolId), node, protocolId);
  }

  @Override
  public Object clone() {
    return new SeedMessageHandler(this.prefix);  // Use the stored prefix to create a new instance
  }
}
