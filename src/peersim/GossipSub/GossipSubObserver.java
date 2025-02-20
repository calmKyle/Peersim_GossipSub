package peersim.GossipSub;
//*************************Same Class as Kademlia******
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalStats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class GossipSubObserver implements Control{
    /**
     * This class implements a simple observer of search time and hop average in finding a node in the network
     *
     * @author Daniele Furlan, Maurizio Bonani
     * @version 1.0
     */
    /** Parameter of the protocol we want to observe */
    private static final String PAR_PROT = "protocol";
    /**
     * keep statistics of the time every message delivered.
     */
    public static IncrementalStats timeStore = new IncrementalStats();
    /**
     * keep statistic of number of message delivered
     */
    public static IncrementalStats msg_deliv = new IncrementalStats();
    /** Protocol id */
    private int pid;

    /** Prefix to be printed in output */
    private String prefix;

    private int proposerStratergy;

    public GossipSubObserver(String prefix) {
        this.prefix = prefix;
        pid = Configuration.getPid(prefix + "." + PAR_PROT);
        proposerStratergy = Configuration.getInt("DISTRIBUTION_STRATEGY");
    }

    /**
     * print the statistical snapshot of the current situation
     *
     * @return boolean always false
     */
    public boolean execute() {
        String filePath = "output2.csv";
        boolean append = CommonState.getTime() > 0; // Always append for times > 0

        File file = new File(filePath);
//        System.out.println("Trying to delete the file: " + file.getAbsolutePath());
        if (file.exists() && CommonState.getTime() == 0) {
            file.delete();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, append))) {
            // Write the header only if it's the first time writing to the file
            if (!append) {
                writer.write("Time,Node ID,Custody 1,Custody 2,Seed Arrival Times,Seed RTT Times,Min Seed RTT,Avg Seed RTT,Max Seed RTT,"
                        + "Total Sample Req Sent,Total Sample Received,Total Sample Req Timedout,"
                        + "Sample Arrival Times,Sample RTT Times,Min Sample RTT,Avg Sample RTT,Max Sample RTT,"
                        + "Total Seed Parts Received,Seed Part Arrival Times,Seed Part RTT Times,Min Seed Part RTT,"
                        + "Avg Seed Part RTT,Max Seed Part RTT,Avg. Bandwidth");
                writer.newLine();
                return false;
            }

            int count = 0;
            for (Node nd : CustomDistribution.networkNodes.values()) {
                if (count == Configuration.getInt("NUMBER_OF_VALIDATORS")) {
                    break;
                }
                if (nd == CustomDistribution.blockProposerNode) {
                    continue;
                }
                GossipSubProtocol protocol = (GossipSubProtocol) (nd.getProtocol(pid));

                double minSeedRTT = protocol.seedArrivalTimeStore == null ? 0.0 : protocol.seedArrivalTimeStore.getMin();
                double avgSeedRTT = protocol.seedArrivalTimeStore == null ? 0.0 : protocol.seedArrivalTimeStore.getAverage();
                double maxSeedRTT = protocol.seedArrivalTimeStore == null ? 0.0 : protocol.seedArrivalTimeStore.getMax();

                double minSampleRTT = protocol.samplingRTTTimeStore == null ? 0.0 : protocol.samplingRTTTimeStore.getMin();
                double avgSampleRTT = protocol.samplingRTTTimeStore == null ? 0.0 : protocol.samplingRTTTimeStore.getAverage();
                double maxSampleRTT = protocol.samplingRTTTimeStore == null ? 0.0 : protocol.samplingRTTTimeStore.getMax();
                String metrics1 = String.format(
                        "[time=%d] Node %d: [Seed Arrival Times=%s] [Seed RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                        CommonState.getTime(),
                        count,
                        protocol.messageArrivalTimeFromBP,
                        protocol.messageDelayTimeFromBP,
                        protocol.seedArrivalTimeStore.getMin(),
                        protocol.seedArrivalTimeStore.getAverage(),
                        protocol.seedArrivalTimeStore.getMax()
                );
                System.out.println(metrics1);
                if(proposerStratergy==2)
                {
                    String metrics2 = String.format(
                            "[time=%d] Node %d: [Total Seed Parts Recieved=%d] [Seed Part Arrival Times=%s] [Seed Part RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                            CommonState.getTime(),
                            count,
                            protocol.noOfSeedPartsReceived,
                            protocol.seedPartArrivalTimeFromPeer,
                            protocol.seedPartDelayTimeFromPeer,
                            protocol.seedPartArrivalTimeStore.getMin(),
                            protocol.seedPartArrivalTimeStore.getAverage(),
                            protocol.seedPartArrivalTimeStore.getMax()
                    );
                    System.out.println(metrics2);
                }

                String metrics3 = String.format(
                        "[time=%d] Node Idx No %d: [Total Sample Req Sent=%d] [Total Sample Recieved=%d] [Total Sample Req Timedout=%d]\n[Sample Arrival Times=%s]\n[Sample RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                        CommonState.getTime(),
                        count, //This is the index number at which this node if present in the CustomDistribution.networkNodes
                        protocol.noOfSampleRequestsSent,
                        protocol.noOfSamplesReceived,
                        protocol.sampleRequestUnsuccessful,
                        protocol.sampleArrivalTime,
                        protocol.sampleDelayTime,
                        protocol.samplingRTTTimeStore.getMin(),
                        protocol.samplingRTTTimeStore.getAverage(),
                        protocol.samplingRTTTimeStore.getMax()
                );
                System.out.println(metrics3);
                String metrics4 = String.format(
                        "[average bandwidth=%f]",
                        ((double)protocol.totalDataTransmitted /(double) (protocol.totalTransmissionTime)*1000)
                );
                System.out.println(metrics4);
                System.out.println();
                System.out.println();

                Collections.sort(protocol.messageArrivalTimeFromBP);
                Collections.sort(protocol.messageDelayTimeFromBP);
                Collections.sort(protocol.sampleArrivalTime);
                Collections.sort(protocol.sampleDelayTime);
                Collections.sort(protocol.seedPartArrivalTimeFromPeer);
                Collections.sort(protocol.seedPartDelayTimeFromPeer);

                String seedArrivalTimes = protocol.messageArrivalTimeFromBP.isEmpty() ? "[]" : Arrays.toString(protocol.messageArrivalTimeFromBP.toArray())
                        .replace("[", "")
                        .replace("]", "")
                        .replace(",", ";");

                String seedMessageDelayTimes = protocol.messageDelayTimeFromBP.isEmpty() ? "[]" : Arrays.toString(protocol.messageDelayTimeFromBP.toArray())
                        .replace("[", "")
                        .replace("]", "")
                        .replace(",", ";");

                // Process sample arrival times and delay times
                String sampleArrivalTimes = protocol.sampleArrivalTime.isEmpty() ? "[]" : Arrays.toString(protocol.sampleArrivalTime.toArray())
                        .replace("[", "")
                        .replace("]", "")
                        .replace(",", ";");

                String sampleMessageDelayTimes = protocol.sampleDelayTime.isEmpty() ? "[]" : Arrays.toString(protocol.sampleDelayTime.toArray())
                        .replace("[", "")
                        .replace("]", "")
                        .replace(",", ";");


                String seedPartArrivalTimes = proposerStratergy == 2 && !protocol.seedPartArrivalTimeFromPeer.isEmpty()
                        ? Arrays.toString(protocol.seedPartArrivalTimeFromPeer.toArray()).replace("[", "").replace("]", "").replace(",", ";")
                        : "[]";

                String seedPartDelayTimes = proposerStratergy == 2 && !protocol.seedPartDelayTimeFromPeer.isEmpty()
                        ? Arrays.toString(protocol.seedPartDelayTimeFromPeer.toArray()).replace("[", "").replace("]", "").replace(",", ";")
                        : "[]";

                String metrics = String.format(
                        "%d,%d,%s,%s,%s,%s,%f,%f,%f,%d,%d,%d,%s,%s,%f,%f,%f,%d,%s,%s,%f,%f,%f",
                        CommonState.getTime(),
                        count,
                        protocol.custody1,
                        protocol.custody2,
                        seedArrivalTimes,
                        seedMessageDelayTimes,
                        minSeedRTT,
                        avgSeedRTT,
                        maxSeedRTT,
                        protocol.noOfSampleRequestsSent,
                        protocol.noOfSamplesReceived,
                        protocol.sampleRequestUnsuccessful,
                        sampleArrivalTimes,
                        sampleMessageDelayTimes,
                        minSampleRTT,
                        avgSampleRTT,
                        maxSampleRTT,
                        proposerStratergy == 2 ? protocol.noOfSeedPartsReceived : 0,
                        seedPartArrivalTimes,
                        seedPartDelayTimes,
                        proposerStratergy == 2 ? protocol.seedPartArrivalTimeStore.getMin() : 0.0,
                        proposerStratergy == 2 ? protocol.seedPartArrivalTimeStore.getAverage() : 0.0,
                        proposerStratergy == 2 ? protocol.seedPartArrivalTimeStore.getMax() : 0.0,
                        (double)protocol.totalDataTransmitted / (double)(protocol.totalTransmissionTime*1000)
                );

                writer.write(metrics);
                writer.newLine();

                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

}
