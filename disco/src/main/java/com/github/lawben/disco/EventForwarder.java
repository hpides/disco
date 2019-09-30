package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.STREAM_END;

import org.zeromq.ZMQ;

public class EventForwarder extends DistributedNode implements Runnable {
    protected final static String NODE_IDENTIFIER = "FORWARDER";

    public EventForwarder(int nodeId, int controllerPort, int dataPort, int numChildren,
            String parentIp, int parentControllerPort, int parentWindowPort) {
        super(nodeId, NODE_IDENTIFIER, controllerPort, dataPort, numChildren,
                parentIp, parentControllerPort, parentWindowPort);
        this.createDataPuller();
        this.createWindowPusher(parentIp, parentWindowPort);
    }

    @Override
    public void run() {
        System.out.println(this.nodeString("Starting merge worker with window port " + this.dataPort +
                " and controller port " + this.controllerPort + ", with " + this.numChildren +
                " children. Connecting to parent at " + this.parentIp + " with controller port " +
                this.parentControllerPort + " and window port " + this.parentWindowPort));

        try {
            this.registerAtParent();
            this.waitForChildren();
            forwardEvents();
        } finally {
            this.close();
        }
    }

    private void forwardEvents() {
        ZMQ.Socket streamInput = dataPuller;
        ZMQ.Socket streamOutput = windowPusher;
        System.out.println(this.nodeString("Starting forwarding."));

        long numEventsForwarded = 0;

        while (!this.isInterrupted()) {
            String eventOrStreamEnd = streamInput.recvStr();
            if (eventOrStreamEnd == null) {
                continue;
            }

            if (eventOrStreamEnd.equals(STREAM_END)) {
                if (!this.isTotalStreamEnd()) {
                    continue;
                }

                System.out.println(this.nodeString("Forwarded " + numEventsForwarded + " events in total."));
                System.out.println(this.nodeString("No more children. Shutting down..."));
                this.endChild();
                return;
            }

            streamOutput.send(eventOrStreamEnd);
            numEventsForwarded++;

            if (numEventsForwarded % 1_000_000 == 0) {
                System.out.println("Forwarded " + numEventsForwarded + " events.");
            }
        }
    }
}
