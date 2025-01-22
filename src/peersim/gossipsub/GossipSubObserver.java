package peersim.gossipsub;
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
import java.util.Collections;

public class GossipSubObserver implements Control{


    /**
     * This class implements a simple observer of search time and hop average in finding a node in the network
     *
     * @author Daniele Furlan, Maurizio Bonani
     * @version 1.0
     */
        /**
         * keep statistics of the time every message delivered.
         */
        public static IncrementalStats timeStore = new IncrementalStats();

        /**
         * keep statistic of number of message delivered
         */
        public static IncrementalStats msg_deliv = new IncrementalStats();


        /** Parameter of the protocol we want to observe */
        private static final String PAR_PROT = "protocol";

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
            int count=0;
            for (Node nd : CustomDistribution.networkNodes.values()) // Looping over all the topics
            {
                if(count==1024)
                {
                    break;
                }
                if(nd==CustomDistribution.blockProposerNode)
                {
                    continue;
                }
                GossipSubProtocol protocol =(GossipSubProtocol) (nd.getProtocol(pid));

                StringBuilder seedArrivalTimes = new StringBuilder();
                StringBuilder seedMessageDelayTimes = new StringBuilder();
                seedArrivalTimes.append("[");
                seedMessageDelayTimes.append("[");
                for(int i=0;i<protocol.messageArrivalTimeFromBP.size();i++)
                {
                    seedArrivalTimes.append(protocol.messageArrivalTimeFromBP.get(i)).append(", ");
                    seedMessageDelayTimes.append(protocol.messageDelayTimeFromBP.get(i)).append(", ");
                }

                // Removing the ending comma and space
                if (seedArrivalTimes.length() > 1) {
                    seedArrivalTimes.setLength(seedArrivalTimes.length() - 2);
                    seedMessageDelayTimes.setLength(seedMessageDelayTimes.length() - 2);
                }
                seedArrivalTimes.append("]");
                seedMessageDelayTimes.append("]");

                String metrics = String.format(
                        "[time=%d] Node %d: [Seed Arrival Times=%s] [Seed RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                        CommonState.getTime(),
                        protocol.nodeId,
                        seedArrivalTimes,
                        seedMessageDelayTimes,
                        protocol.seedArrivalTimeStore.getMin(),
                        protocol.seedArrivalTimeStore.getAverage(),
                        protocol.seedArrivalTimeStore.getMax()
                );
                System.out.println(metrics);
                count++;
            }
            count=0;
            for (Node nd : CustomDistribution.networkNodes.values()) // Looping over all the topics
            {
                if(count==1024)
                {
                    break;
                }
                if(nd==CustomDistribution.blockProposerNode)
                {
                    continue;
                }
                GossipSubProtocol protocol =(GossipSubProtocol) (nd.getProtocol(pid));

                StringBuilder sampleArrivalTimes = new StringBuilder();
                StringBuilder sampleMessageDelayTimes = new StringBuilder();
                sampleArrivalTimes.append("[");
                sampleMessageDelayTimes.append("[");

                StringBuilder seedPartArrivalTimes = new StringBuilder();
                StringBuilder seedPartDelayTimes = new StringBuilder();
                seedPartArrivalTimes.append("[");
                seedPartDelayTimes.append("[");
                Collections.sort(protocol.sampleArrivalTime);
                Collections.sort(protocol.sampleDelayTime);
                for(int i=0;i<protocol.sampleArrivalTime.size();i++)
                {
                    sampleArrivalTimes.append(protocol.sampleArrivalTime.get(i)).append(", ");
                    sampleMessageDelayTimes.append(protocol.sampleDelayTime.get(i)).append(", ");
                }
                if(proposerStratergy==2)
                {
                    for(int i=0;i<protocol.seedPartArrivalTimeFromPeer.size();i++)
                    {
                    seedPartArrivalTimes.append(protocol.seedPartArrivalTimeFromPeer.get(i)).append(", ");
                    seedPartDelayTimes.append(protocol.seedPartDelayTimeFromPeer.get(i)).append(", ");
                    }
                }


                // Removing the ending comma and space
                if (sampleArrivalTimes.length() > 1) {
                    sampleArrivalTimes.setLength(sampleArrivalTimes.length() - 2);
                    sampleMessageDelayTimes.setLength(sampleMessageDelayTimes.length() - 2);
                }
                if(seedPartArrivalTimes.length()>1) {
                    seedPartArrivalTimes.setLength(seedPartArrivalTimes.length() - 2);
                    seedPartDelayTimes.setLength(seedPartDelayTimes.length() - 2);
                }
                sampleArrivalTimes.append("]");
                sampleMessageDelayTimes.append("]");
                seedPartArrivalTimes.append("]");
                seedPartDelayTimes.append("]");

                if(proposerStratergy==2)
                {
                    String metrics1 = String.format(
                            "[time=%d] Node %d: [Total Seed Parts Recieved=%d] [Seed Part Arrival Times=%s] [Seed Part RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                            CommonState.getTime(),
                            protocol.nodeId,
                            protocol.NoOfSeedPartsRecieved,
                            seedPartArrivalTimes,
                            seedPartDelayTimes,
                            protocol.seedPartArrivalTimeStore.getMin(),
                            protocol.seedPartArrivalTimeStore.getAverage(),
                            protocol.seedPartArrivalTimeStore.getMax()
                    );
                    System.out.println(metrics1);
                }

                String metrics2 = String.format(
                        "[time=%d] Node %d: [Total Sample Req Sent=%d] [Total Sample Recieved=%d] [Total Sample Req Timedout=%d] [Sample Arrival Times=%s] [Sample RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                        CommonState.getTime(),
                        protocol.nodeId,
                        protocol.NoOfSampleRequestsSent,
                        protocol.NoOfSamplesRecieved,
                        protocol.sampleRequestUnsuccessful,
                        sampleArrivalTimes,
                        sampleMessageDelayTimes,
                        protocol.samplingRTTTimeStore.getMin(),
                        protocol.samplingRTTTimeStore.getAverage(),
                        protocol.samplingRTTTimeStore.getMax()
                );
                System.out.println(metrics2);
                System.out.println();
                System.out.println();
                count++;
            }
            return false;
        }
    }

//TO DOs
// Make these configurable
//2. Number of topics (Done)
//3. Number of rows (Done)
//4. Number of columns (Done)
//5. Matrix size (Done)
//7. Number of validators (Done)
//8. maximumBandwidth (Done)
//9. partRequestCounter in GossipSubProtocol2 (LEFT)
//1. Number of copies of each row/col that are distributed by the block producer (LEFT)
//6. If whole/complete row/col to be sent to individual node or not (LEFT)



//1. RTT(Round trip time)
//2. Latency(Time it took to recieve the data/sample after the request message was sent)
//3. Time it takes distribute the rows/cols and the time it takes for sampling
//4. Number of messages delivered(show stats for: type of message, number of hops)

//5. Number of Retransmissions (no need as the transport protocol is reliable/no data loss )
//6. Number of packet/sample loss (no need as the transport protocol is reliable/no data loss )

