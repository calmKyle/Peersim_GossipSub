package peersim.GossipSub.PANDAS;

import peersim.core.*;
import peersim.config.*;
import peersim.edsim.*;
import peersim.GossipSub.Block;
import peersim.GossipSub.TopicBasedMesh;

import java.security.KeyPair;
import java.util.HashMap;
import java.util.Map;

public class PANDASProtocol implements EDProtocol {

    private static final String PAR_BUILDER = "builder";
    private SeedMessageHandler seedHandler;
    private ForkChoiceStrategy forkChoiceStrategy;

    // Maintain a mapping of blockID → Block object
    private final Map<Integer, Block> blockStorage = new HashMap<>();

    public PANDASProtocol(String prefix) {
        // Initialization from config file
        this.seedHandler = new SeedMessageHandler(prefix);

        String strategy = Configuration.getString(prefix + ".forkChoiceStrategy", "TightForkChoice");
        switch (strategy) {
            case "CasperFFG":
                this.forkChoiceStrategy = new CasperFFG();
                break;
            case "LMDGHOST":
                this.forkChoiceStrategy = new LMDGHOST();
                break;
            case "TightForkChoice":
            default:
                this.forkChoiceStrategy = new TightForkChoice();
                break;
        }
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
        if (event instanceof SeedMessage) {
            handleSeedMessage(node, (SeedMessage) event);
        } else if (event instanceof SampleMessage) {
            handleSampleMessage(node, (SampleMessage) event);
        } else if (event instanceof VoteMessage) {
            handleVoteMessage(node, (VoteMessage) event);
        } else if (event instanceof ConsensusMessage) {
            handleConsensusMessage(node, (ConsensusMessage) event);
        }
    }

    private void handleSeedMessage(Node node, SeedMessage msg) {
        System.out.println("Seed message received by Node " + node.getID() + " from " + msg.getSender().getID());

        // Check if block already exists
        if (blockStorage.containsKey(msg.getBlockID())) {
            System.out.println("Block already exists. Ignoring.");
            return;
        }

        try {
            // Generate a new key pair for this block
            KeyPair validatorKeyPair = SignatureValidator.generateKeyPair();

            // Create a new block with the generated key pair
            Block newBlock = new Block(msg.getRows(), msg.getColumns(), validatorKeyPair);
            newBlock.initialiseDataMatrix();
            blockStorage.put(msg.getBlockID(), newBlock);

            System.out.println("New block created with ID: " + newBlock.blockID + " at Node " + node.getID());

            // Check if seed data is valid
            if (seedHandler.verifyData(msg.getData())) {
                System.out.println("Data is valid.");
                // Further processing or forwarding
            } else {
                System.out.println("Received data is invalid.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error generating block at Node " + node.getID());
        }
    }

    private void handleSampleMessage(Node node, SampleMessage msg) {
        // Retrieve block from storage
        Block block = blockStorage.get(msg.getBlockID());
        if (block == null) {
            System.out.println("Block ID " + msg.getBlockID() + " not found at Node " + node.getID());
            return;
        }

        byte[] sampleData = block.getSample(msg.getRow(), msg.getColumn());
        System.out.println("Sample message processed with data sample at Node " + node.getID());

        // Additional validation logic
        if (validateSample(sampleData)) {
            System.out.println("Sample data validated successfully.");
        } else {
            System.out.println("Sample data validation failed.");
        }
    }

    private boolean validateSample(byte[] data) {
        if (data == null)
            return false;
        for (byte b : data) {
            if (b != 0)
                return true; // At least one byte is non-zero
        }
        return false;
    }

    private void handleVoteMessage(Node node, VoteMessage msg) {
        System.out.println("Vote message received at Node " + node.getID() + " for Block " + msg.getBlockID());
        // TODO: Implement voting logic
    }

    private void handleConsensusMessage(Node node, ConsensusMessage msg) {
        System.out.println("Consensus message received at Node " + node.getID());

        // Re-organize the mesh based on consensus data
        TopicBasedMesh meshManager = new TopicBasedMesh("configPrefix");
        meshManager.createTopicMesh();
        System.out.println("Network topology updated at Node " + node.getID());
    }

    @Override
    public Object clone() {
        return new PANDASProtocol("configPrefix");
    }
}
