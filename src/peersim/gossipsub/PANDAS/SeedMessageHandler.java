package peersim.gossipsub;

import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;

/**
 * Class to handle seed messages within the PANDAS protocol.
 */
public class SeedMessageHandler implements EDProtocol {

  public static final String PAR_PROT = "protocol";

  private final int protocolId;

  public SeedMessageHandler(String prefix) {
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
    System.out.println("Node " + node.getID() + " received a seed from " + msg.sender.getID());
    // Example of handling data: simply printing or storing the data
    // This should be expanded based on actual requirements of data consolidation or
    // verification

    // Assuming `data` needs to be consolidated or verified
    if (verifyData(msg.data)) {
      System.out.println("Data verified successfully at Node " + node.getID());
      // Potentially forwarding or further processing the data
      forwardData(node, msg.data);
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
  private boolean verifyData(Object data) {
    // Implement verification logic here
    // This is a placeholder for data verification logic
    return true; // Assuming verification is always successful for demonstration
  }

  /**
   * Forwards the data to other nodes or performs further processing.
   * 
   * @param node the current node
   * @param data the data to forward
   */
  private void forwardData(Node node, Object data) {
    // Implement data forwarding or additional processing logic here
    // This could involve sending data to other nodes or processing it locally
    System.out.println("Node " + node.getID() + " is forwarding data.");
    // For example, simulate forwarding with a delay
    EDSimulator.add(100, new SeedMessage(node, data), node, protocolId);
  }

  @Override
  public Object clone() {
    return new SeedMessageHandler();
  }
}
