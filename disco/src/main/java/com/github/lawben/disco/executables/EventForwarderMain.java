package com.github.lawben.disco.executables;

import com.github.lawben.disco.EventForwarder;

public class EventForwarderMain {
    public static void main(String[] args) {
        if (args.length < 7) {
            System.out.println("Not enough arguments!\nUsage: java ... parentIp parentControllerPort parentWindowPort "
                                                                    + "controllerPort windowPort numChildren nodeId");
            System.exit(1);
        }

        final String parentIp = args[0];
        final int parentControllerPort = Integer.parseInt(args[1]);
        final int parentWindowPort = Integer.parseInt(args[2]);
        final int controllerPort = Integer.parseInt(args[3]);
        final int windowPort = Integer.parseInt(args[4]);
        final int numChildren = Integer.parseInt(args[5]);
        final int nodeId = Integer.parseInt(args[6]);

        runForwarder(nodeId, controllerPort, windowPort, numChildren, parentIp, parentControllerPort, parentWindowPort);
    }

    public static void runForwarder(int nodeId, int controllerPort, int dataPort, int numChildren,
                                    String parentIp, int parentControllerPort, int parentWindowPort) {
        EventForwarder worker = new EventForwarder(nodeId, controllerPort, dataPort, numChildren,
                                                   parentIp, parentControllerPort, parentWindowPort);
        Thread thread = new Thread(worker);
        thread.start();
    }
}
