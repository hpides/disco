package com.github.lawben.disco.executables;

import com.github.lawben.disco.DistributedChild;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;

public class TsharkRunner {
    // Example invocation:
    // tshark -f \"tcp port 4055 || tcp port 4056 || tcp portrange "4060-4069 || tcp portrange 4160-4169\"
    // -i lo0 -w /tmp/single-run-4-20-1000000.pcap (-a duration:300)
    private static final String TSHARK_PORT_TEMPLATE = "tcp port %d || tcp port %d || tcp portrange %d-%d || tcp portrange %d-%d";


    public static Process startTshark(int rootControllerPort, int rootWindowPort, int streamPort,
                                      int numChildren, int numStreams, int numEvents, String filePrefix, String networkInterface) throws IOException {
        final int streamRegisterPort = streamPort + DistributedChild.STREAM_REGISTER_PORT_OFFSET;
        Instant runTimestamp = Instant.ofEpochMilli(System.currentTimeMillis());
        final String tsharkOutputFile = String.format("/Users/law/repos/ma/runs-scotty/%s-run-%d-%d-%d-%s.pcap",
                filePrefix, numChildren, numStreams, numEvents, runTimestamp);

        String tsharkPortString = String.format(TSHARK_PORT_TEMPLATE, rootControllerPort, rootWindowPort, streamPort,
                streamPort + 10, streamRegisterPort, streamRegisterPort + 10);
        String[] tsharkArgs = {"tshark", "-f", tsharkPortString, "-i", networkInterface, "-i", "lo0", "-w", tsharkOutputFile};
        System.out.println("Starting tshark with command:\n\t" + Arrays.toString(tsharkArgs));
        return Runtime.getRuntime().exec(tsharkArgs);
    }

    public static void endTshark(Process tshark) throws IOException {
        System.out.println("TSHARK OUT:\n-----------");
        printTsharkOut(tshark.getErrorStream());
        printTsharkOut(tshark.getInputStream());
        tshark.destroy();
    }

    private static void printTsharkOut(java.io.InputStream in) throws IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(in));
        while (input.ready()) {
            System.out.println(input.readLine());
        }
    }
}
