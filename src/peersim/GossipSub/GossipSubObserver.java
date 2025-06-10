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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class GossipSubObserver implements Control {
    /**
     * This class implements a simple observer of search time and hop average in
     * finding a node in the network
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

    /** nodeId → { lastComeIHAVE , lastComeIWANT } */
    private final Map<BigInteger, long[]> lastTotals = new HashMap<>();

    /** nodeId → Writer for Node_Message/node_<id>.csv */
    private final Map<BigInteger, BufferedWriter> nodeWriters = new HashMap<>();

    /** Writer for Total_message/total_messages.csv */
    private BufferedWriter totalWriter;

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

        int t = (int) CommonState.getTime();
        printResult();
        dumpIHAVE_IWANT(t, pid);
        printMesh();

        return false;
    }

    private void printResult() {
        File directory = new File("CSVOut");
        if (!directory.exists()) {
            directory.mkdirs();
        }
        double currentMaliciousRateID = Configuration.getDouble("MALICIOUS_RATE", 0.0);
        double seedID = Configuration.getDouble("random.seed", 0.0);
        String filePath = "CSVOut/output_results_malicious_rate_" + currentMaliciousRateID + "_seed_" + seedID + ".csv";

        boolean append = CommonState.getTime() > 0; // Always append for times > 0

        File file = new File(filePath);
        if (file.exists() && CommonState.getTime() == 0) {
            file.delete();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, append))) {
            // Write the header only if it's the first time writing to the file
            if (!append) {
                writer.write(
                        "Time,Node ID,Custody 1,Custody 2,Seed Arrival Times,Seed RTT Times,Min Seed RTT,Avg Seed RTT,Max Seed RTT,"
                                + "Total Sample Req Sent,Total Sample Received,Total Sample Req Timedout,"
                                + "Sample Arrival Times,Sample RTT Times,Min Sample RTT,Avg Sample RTT,Max Sample RTT,"
                                + "Total Seed Parts Received,Seed Part Arrival Times,Seed Part RTT Times,Min Seed Part RTT,"
                                + "Avg Seed Part RTT,Max Seed Part RTT,Avg. Bandwidth, Duplicated Message IHAVE");
                writer.newLine();
                return;
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

                double minSeedRTT = protocol.seedArrivalTimeStore == null ? 0.0
                        : protocol.seedArrivalTimeStore.getMin();
                double avgSeedRTT = protocol.seedArrivalTimeStore == null ? 0.0
                        : protocol.seedArrivalTimeStore.getAverage();
                double maxSeedRTT = protocol.seedArrivalTimeStore == null ? 0.0
                        : protocol.seedArrivalTimeStore.getMax();

                double minSampleRTT = protocol.samplingRTTTimeStore == null ? 0.0
                        : protocol.samplingRTTTimeStore.getMin();
                double avgSampleRTT = protocol.samplingRTTTimeStore == null ? 0.0
                        : protocol.samplingRTTTimeStore.getAverage();
                double maxSampleRTT = protocol.samplingRTTTimeStore == null ? 0.0
                        : protocol.samplingRTTTimeStore.getMax();
                String metrics1 = String.format(
                        "[time=%d] Node %d: [Seed Arrival Times=%s] [Seed RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                        CommonState.getTime(),
                        count,
                        protocol.messageArrivalTimeFromBP,
                        protocol.messageDelayTimeFromBP,
                        protocol.seedArrivalTimeStore.getMin(),
                        protocol.seedArrivalTimeStore.getAverage(),
                        protocol.seedArrivalTimeStore.getMax());
                // System.out.println(metrics1);
                if (proposerStratergy == 2) {
                    String metrics2 = String.format(
                            "[time=%d] Node %d: [Total Seed Parts Recieved=%d] [Seed Part Arrival Times=%s] [Seed Part RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                            CommonState.getTime(),
                            count,
                            protocol.noOfSeedPartsReceived,
                            protocol.seedPartArrivalTimeFromPeer,
                            protocol.seedPartDelayTimeFromPeer,
                            protocol.seedPartArrivalTimeStore.getMin(),
                            protocol.seedPartArrivalTimeStore.getAverage(),
                            protocol.seedPartArrivalTimeStore.getMax());
                    // System.out.println(metrics2);
                }

                String metrics3 = String.format(
                        "[time=%d] Node Idx No %d: [Total Sample Req Sent=%d] [Total Sample Recieved=%d] [Total Sample Req Timedout=%d]\n[Sample Arrival Times=%s]\n[Sample RTT times=%s] [%f min ] [%f msec average ] [%f max ]",
                        CommonState.getTime(),
                        count, // This is the index number at which this node if present in the
                               // CustomDistribution.networkNodes
                        protocol.noOfSampleRequestsSent,
                        protocol.noOfSamplesReceived,
                        protocol.sampleRequestUnsuccessful,
                        protocol.sampleArrivalTime,
                        protocol.sampleDelayTime,
                        protocol.samplingRTTTimeStore.getMin(),
                        protocol.samplingRTTTimeStore.getAverage(),
                        protocol.samplingRTTTimeStore.getMax());
                // System.out.println(metrics3);
                double bw = protocol.getAverageBandwidthKBps();
                String metrics4 = String.format(
                        "[average bandwidth=%f]", bw);
                // System.out.println(metrics4);
                // System.out.println();
                // System.out.println();

                Collections.sort(protocol.messageArrivalTimeFromBP);
                Collections.sort(protocol.messageDelayTimeFromBP);
                Collections.sort(protocol.sampleArrivalTime);
                Collections.sort(protocol.sampleDelayTime);
                Collections.sort(protocol.seedPartArrivalTimeFromPeer);
                Collections.sort(protocol.seedPartDelayTimeFromPeer);

                String seedArrivalTimes = protocol.messageArrivalTimeFromBP.isEmpty() ? "[]"
                        : Arrays.toString(protocol.messageArrivalTimeFromBP.toArray())
                                .replace("[", "")
                                .replace("]", "")
                                .replace(",", ";");

                String seedMessageDelayTimes = protocol.messageDelayTimeFromBP.isEmpty() ? "[]"
                        : Arrays.toString(protocol.messageDelayTimeFromBP.toArray())
                                .replace("[", "")
                                .replace("]", "")
                                .replace(",", ";");

                // Process sample arrival times and delay times
                String sampleArrivalTimes = protocol.sampleArrivalTime.isEmpty() ? "[]"
                        : Arrays.toString(protocol.sampleArrivalTime.toArray())
                                .replace("[", "")
                                .replace("]", "")
                                .replace(",", ";");

                String sampleMessageDelayTimes = protocol.sampleDelayTime.isEmpty() ? "[]"
                        : Arrays.toString(protocol.sampleDelayTime.toArray())
                                .replace("[", "")
                                .replace("]", "")
                                .replace(",", ";");

                String seedPartArrivalTimes = proposerStratergy == 2 && !protocol.seedPartArrivalTimeFromPeer.isEmpty()
                        ? Arrays.toString(protocol.seedPartArrivalTimeFromPeer.toArray()).replace("[", "")
                                .replace("]", "").replace(",", ";")
                        : "[]";

                String seedPartDelayTimes = proposerStratergy == 2 && !protocol.seedPartDelayTimeFromPeer.isEmpty()
                        ? Arrays.toString(protocol.seedPartDelayTimeFromPeer.toArray()).replace("[", "")
                                .replace("]", "").replace(",", ";")
                        : "[]";

                String metrics = String.format(
                        "%d,%d,%s,%s,%s,%s,%f,%f,%f,%d,%d,%d,%s,%s,%f,%f,%f,%d,%s,%s,%f,%f,%f,%f,%f",
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
                        bw,
                        (double) protocol.duplicateData);

                writer.write(metrics);
                writer.newLine();

                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printMesh() {
        final String meshFilePath = "mesh_connections.csv";
        long t = CommonState.getTime();
        boolean append = t > 0;

        File f = new File(meshFilePath);
        if (!append && f.exists())
            f.delete(); // fresh file at t=0

        try (BufferedWriter w = new BufferedWriter(new FileWriter(f, append))) {
            if (!append) {
                w.write("Source,Target,Type,Weight,Topic");
                w.newLine();
            }

            for (Node node : CustomDistribution.networkNodes.values()) {
                GossipSubProtocol gsp = (GossipSubProtocol) node.getProtocol(pid);
                BigInteger srcId = gsp.getNodeId();

                // walk this node's *localMesh* (active peers)
                for (Map.Entry<String, Set<BigInteger>> entry : gsp.localMesh.entrySet()) {
                    String topic = entry.getKey();
                    for (BigInteger dstId : entry.getValue()) {
                        w.write(srcId + "," + dstId + ",Directed,1," + topic);
                        w.newLine();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Dump one CSV row per node *and* one aggregated row for the whole
     * network for the current simulation tick.
     *
     * @param t   current tick (CommonState.getTime())
     * @param pid protocol id that holds GossipSubProtocol
     */
//    private void dumpIHAVE_IWANT(int t, int pid) {
//
//        long netRecvIWANT = 0, netSendIHAVE = 0;
//
//        for (int i = 0; i < Network.size(); i++) {
//            Node n = Network.get(i);
//            GossipSubProtocol p = (GossipSubProtocol) n.getProtocol(pid);
//
//            long curRecvIWANT = p.iWantRecv;
//            long curSendIHAVE = p.iHaveSent;
//
//            long[] last = lastTotals.computeIfAbsent(p.nodeId, k -> new long[2]);
//            long dRecvIWANT = curRecvIWANT - last[0];
//            long dSendIHAVE = curSendIHAVE - last[1];
//            last[0] = curRecvIWANT;
//            last[1] = curSendIHAVE;
//
//            netRecvIWANT += dRecvIWANT;
//            netSendIHAVE += dSendIHAVE;
//
//            BufferedWriter bw = nodeWriters.get(p.nodeId);
//            try {
//                if (bw == null) {
//                    Path f = Paths.get("Node_Message", "node_" + p.nodeId + ".csv");
//                    bw = Files.newBufferedWriter(f,
//                            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
//                    bw.write("t,RECV_IWANT,SENT_IHAVE\n");
//                    nodeWriters.put(p.nodeId, bw);
//                }
//                bw.write(t + "," + dRecvIWANT + "," + dSendIHAVE + '\n');
//                bw.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        try {
//            if (totalWriter == null) {
//                Path f = Paths.get("Total_message", "total_messages.csv");
//                totalWriter = Files.newBufferedWriter(f,
//                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
//                totalWriter.write("t,TOT_RECV_IWANT,TOT_SENT_IHAVE\n");
//            }
//            totalWriter.write(t + "," + netRecvIWANT + "," + netSendIHAVE + '\n');
//            totalWriter.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    private long activeSeed = Long.MIN_VALUE;
    private void dumpIHAVE_IWANT(int t, int pid) {

        /* ---------- 1. Resolve the current seed & folders ---------- */
        long seed = (long) Configuration.getDouble("random.seed", 0.0);   // strip “.0”
        Path baseDir        = Paths.get("CSV_MESSAGE_TYPE_OUT");
        Path nodeSeedDir    = baseDir.resolve("Node_Message_Seed_"  + seed);
        Path totalSeedDir   = baseDir.resolve("Total_Message_Seed_" + seed);

        try {
            Files.createDirectories(nodeSeedDir);
            Files.createDirectories(totalSeedDir);
        } catch (IOException e) {
            e.printStackTrace();
            return;                        // bail out for this tick
        }

        /* ---------- 2. If the seed changed, close & reset writers ---------- */
        if (seed != activeSeed) {
            nodeWriters.values().forEach(bw -> { try { bw.close(); } catch (IOException ignored) {} });
            nodeWriters.clear();
            if (totalWriter != null) {
                try { totalWriter.close(); } catch (IOException ignored) {}
                totalWriter = null;
            }
            activeSeed = seed;             // remember the new seed
        }

        /* ---------- 3. Collect per-node deltas and write CSVs ---------- */
        long netRecvIWANT = 0, netSendIHAVE = 0;

        for (int i = 0; i < Network.size(); i++) {
            Node n = Network.get(i);
            GossipSubProtocol p = (GossipSubProtocol) n.getProtocol(pid);

            long curRecvIWANT = p.iWantRecv;
            long curSendIHAVE = p.iHaveSent;

            long[] last = lastTotals.computeIfAbsent(p.nodeId, k -> new long[2]);
            long dRecvIWANT = curRecvIWANT - last[0];
            long dSendIHAVE = curSendIHAVE - last[1];
            last[0] = curRecvIWANT;
            last[1] = curSendIHAVE;

            netRecvIWANT += dRecvIWANT;
            netSendIHAVE += dSendIHAVE;

            /* ----- per-node file ----- */
            try {
                BufferedWriter bw = nodeWriters.get(p.nodeId);
                if (bw == null) {
                    Path f = nodeSeedDir.resolve("node_" + p.nodeId + ".csv");
                    bw = Files.newBufferedWriter(f,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE);
                    bw.write("t,RECV_IWANT,SENT_IHAVE\n");
                    nodeWriters.put(p.nodeId, bw);
                }
                bw.write(t + "," + dRecvIWANT + "," + dSendIHAVE + '\n');
                bw.flush();
            } catch (IOException e) { e.printStackTrace(); }
        }

        /* ---------- 4. Aggregated “total” file ---------- */
        try {
            if (totalWriter == null) {
                Path f = totalSeedDir.resolve("total_messages.csv");
                totalWriter = Files.newBufferedWriter(f,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
                totalWriter.write("t,TOT_RECV_IWANT,TOT_SENT_IHAVE\n");
            }
            totalWriter.write(t + "," + netRecvIWANT + "," + netSendIHAVE + '\n');
            totalWriter.flush();
        } catch (IOException e) { e.printStackTrace(); }
    }


}
