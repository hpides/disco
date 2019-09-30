package com.github.lawben.disco.executables;

import com.github.lawben.disco.DistributedChild;

public class DistributedChildMain {

    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Not enough arguments!\nUsage: java ... parentIp parentControllerPort parentWindowPort "
                                                                    + "streamPort childId numStreams");
            System.exit(1);
        }

        final String parentIp = args[0];
        final int parentControllerPort = Integer.parseInt(args[1]);
        final int parentWindowPort = Integer.parseInt(args[2]);
        final int streamPort = Integer.parseInt(args[3]);
        final int childId = Integer.parseInt(args[4]);
        final int numStreams = Integer.parseInt(args[5]);

        runChild(parentIp, parentControllerPort, parentWindowPort, streamPort, childId, numStreams);
    }

    public static Thread runChild(String parentIp, int parentControllerPort, int parentWindowPort, int streamPort, int childId, int numStreams) {
        DistributedChild worker = new DistributedChild(parentIp, parentControllerPort, parentWindowPort, streamPort, childId, numStreams);
        Thread thread = new Thread(worker);
        thread.start();
        return thread;
    }
}
