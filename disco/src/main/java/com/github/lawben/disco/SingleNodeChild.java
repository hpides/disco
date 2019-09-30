package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.STREAM_END;

import org.zeromq.ZMQ;

public class SingleNodeChild extends DistributedChild implements Runnable {
    public SingleNodeChild(String parentIp, int parentControllerPort, int parentWindowPort,
                           int streamInputPort, int childId, int numStreams) {
        super(parentIp, parentControllerPort, parentWindowPort, streamInputPort, childId, numStreams);
    }

    @Override
    public void run() {
        System.out.println(nodeImpl.nodeString("Starting child forwarder on port " + nodeImpl.dataPort +
                " with " + nodeImpl.numChildren + " stream(s). Connecting to parent at " +
                nodeImpl.parentIp + " with controller port " + nodeImpl.parentControllerPort +
                " and window port " + nodeImpl.parentWindowPort));

        try {
            WindowingConfig config = this.nodeImpl.registerAtParent();
            boolean registerSuccess = this.registerStreams(config);
            if (!registerSuccess) {
                return;
            }
            this.forwardEvents();
        } finally {
            nodeImpl.close();
        }
    }

    private void forwardEvents() {
        ZMQ.Socket streamInput = nodeImpl.dataPuller;
        ZMQ.Socket streamOutput = nodeImpl.windowPusher;
        System.out.println(nodeImpl.nodeString("Starting child forwarding."));

        long numEventsForwarded = 0;

        while (!nodeImpl.isInterrupted()) {
            String eventOrStreamEnd = streamInput.recvStr();
            if (eventOrStreamEnd == null) {
                continue;
            }

            if (eventOrStreamEnd.equals(STREAM_END)) {
                if (!nodeImpl.isTotalStreamEnd()) {
                    continue;
                }

                System.out.println(nodeImpl.nodeString("Forwarded " + numEventsForwarded + " events in total."));
                System.out.println(nodeImpl.nodeString("No more streams. Shutting down..."));
                nodeImpl.endChild();
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
