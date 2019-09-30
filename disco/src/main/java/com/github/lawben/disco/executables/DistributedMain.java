package com.github.lawben.disco.executables;

public class DistributedMain {
    public static void main(String[] args) throws Exception {
        final int numExpectedArgs = 9;
        if (args.length < numExpectedArgs) {
            System.err.println("Not enough arguments!\nUsage: java ... "
                    + "controllerPort "
                    + "windowPort "
                    + "resultPath "
                    + "streamPortStart "
                    + "numChildren "
                    + "numStreams "
                    + "numEvents "
                    + "windowsString "
                    + "aggFnsString");
            System.exit(1);
        }

        final int rootControllerPort = Integer.parseInt(args[0]);
        final int rootWindowPort = Integer.parseInt(args[1]);
        final String resultPath = args[2];
        final int streamPort = Integer.parseInt(args[3]);
        final int numChildren = Integer.parseInt(args[4]);
        final int numStreams = Integer.parseInt(args[5]);
        final int numEvents = Integer.parseInt(args[6]);
        final String windowsString = args[7];
        final String aggFnsString = args[8];

//        Process tshark = TsharkRunner.startTshark(rootControllerPort, rootWindowPort, streamPort,
//                numChildren, numStreams, numEvents, "dist", "en0");

        // Let tshark start
//        Thread.sleep(2000);

        System.out.println("Running with " + numChildren + " children, " + numStreams + " streams, and " +
                numEvents + " events per stream. Windows: " + windowsString + "; aggFns: " + aggFnsString);

        if (numStreams > 0 && numStreams < numChildren) {
            System.err.println("Need at least as many streams as children! "
                    + "Got " + numStreams + ", need at least " + numChildren);
            System.exit(1);
        }

        Thread rootThread = DistributedRootMain.runRoot(rootControllerPort, rootWindowPort, resultPath, numChildren,
                windowsString, aggFnsString);

        final int numBaseStreamsPerChild = numStreams / numChildren;
        final int numExtraStreams = numStreams % numChildren;
        for (int childId = 0; childId < numChildren; childId++) {
            final int extraChild = childId < numExtraStreams ? 1 : 0;
            final int numStreamsForChild = numBaseStreamsPerChild + extraChild;
            DistributedChildMain.runChild("localhost", rootControllerPort, rootWindowPort, streamPort + childId, childId, numStreamsForChild);
        }

        for (int streamId = 0; streamId < numStreams; streamId++) {
            int assignedChild = streamId % numChildren;
            InputStreamMain.runInputStream(streamId,  "localhost:" + (streamPort + assignedChild), numEvents);
        }

        rootThread.join();
        System.out.println("Finished streaming.");
//        TsharkRunner.endTshark(tshark);
    }
}
