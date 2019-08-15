package com.github.lawben.disco.executables.single;

import com.github.lawben.disco.single.EventForwarder;

public class EventForwarderMain {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Not enough arguments!\nUsage: java ... rootIp rootPort streamPort childId");
            System.exit(1);
        }

        final String rootIp = args[0];
        final int rootPort = Integer.parseInt(args[1]);
        final int streamPort = Integer.parseInt(args[2]);
        final int childId = Integer.parseInt(args[3]);

        runForwarder(rootIp, rootPort, streamPort, childId);
    }

    public static void runForwarder(String rootIp, int rootPort, int streamPort, int childId) {
        EventForwarder worker = new EventForwarder(rootIp, rootPort, streamPort, childId);
        Thread thread = new Thread(worker);
        thread.start();
    }
}
