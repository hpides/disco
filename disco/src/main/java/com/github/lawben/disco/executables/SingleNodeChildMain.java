package com.github.lawben.disco.executables;

import com.github.lawben.disco.SingleNodeChild;

public class SingleNodeChildMain {

    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Not enough arguments!\nUsage: java ... rootIp rootControllerPort rootWindowPort "
                                                                    + "streamPort childId numStreams");
            System.exit(1);
        }

        final String rootIp = args[0];
        final int rootControllerPort = Integer.parseInt(args[1]);
        final int rootWindowPort = Integer.parseInt(args[2]);
        final int streamPort = Integer.parseInt(args[3]);
        final int childId = Integer.parseInt(args[4]);
        final int numStreams = Integer.parseInt(args[5]);

        runChild(rootIp, rootControllerPort, rootWindowPort, streamPort, childId, numStreams);
    }

    public static Thread runChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamPort, int childId, int numStreams) {
        SingleNodeChild worker = new SingleNodeChild(rootIp, rootControllerPort, rootWindowPort,
                                                     streamPort, childId, numStreams);
        Thread thread = new Thread(worker);
        thread.start();
        return thread;
    }
}
