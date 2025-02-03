package peersim.GossipSub;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

/**
 * BandwidthController ensures periodic bandwidth resets for nodes in the
 * network.
 */
public class BandwidthController implements Control {

  private static final String PAR_PROT = "protocol"; // Protocol parameter in config
  private final int pid; // Protocol ID

  /**
   * Constructor to initialize protocol ID.
   */
  public BandwidthController(String prefix) {
    this.pid = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  /**
   * Creates a message to reset bandwidth for a node.
   */
  private Message createBandwidthResetMessage(Node node) {
    Message resetMsg = new Message(Message.MSG_RESET_BANDWIDTH, "reset bandwidth", false, -1, pid, pid);
    resetMsg.timestamp = CommonState.getTime();
    resetMsg.dest = ((GossipSubProtocol) node.getProtocol(pid)).nodeId;
    return resetMsg;
  }

  /**
   * Executes the bandwidth control logic, sending reset messages to all nodes.
   */
  @Override
  public boolean execute() {
    for (Node node : CustomDistribution.networkNodes.values()) {
      EDSimulator.add(0, createBandwidthResetMessage(node), node, pid);
    }
    return false; // Continue execution at the next cycle
  }
}
