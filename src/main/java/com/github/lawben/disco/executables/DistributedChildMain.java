package com.github.lawben.disco.executables;

import com.github.lawben.disco.DistributedChild;

public class DistributedChildMain {

    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Not enough arguments!\nUsage: java ... rootIp rootControllerPort rootWindowPort streamPort childId");
            System.exit(1);
        }

        final String rootIp = args[0];
        final int rootControllerPort = Integer.parseInt(args[1]);
        final int rootWindowPort = Integer.parseInt(args[2]);
        final int streamPort = Integer.parseInt(args[3]);
        final int childId = Integer.parseInt(args[4]);

        runChild(rootIp, rootControllerPort, rootWindowPort, streamPort, childId);
    }

    public static Thread runChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamPort, int childId) {
        DistributedChild worker = new DistributedChild(rootIp, rootControllerPort, rootWindowPort, streamPort, childId);
        Thread thread = new Thread(worker);
        thread.start();
        return thread;
    }
}
