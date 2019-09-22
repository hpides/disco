package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.MAX_LATENESS;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.merge.ChildMerger;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class DistributedChild implements Runnable {
    private final static String NODE_IDENTIFIER = "CHILD";

    private final DistributedNode nodeImpl;

    private long currentEventTime;
    private long lastWatermark;
    private long numEvents;

    public final static int STREAM_REGISTER_PORT_OFFSET = 100;
    public final static long STREAM_REGISTER_TIMEOUT_MS = 10 * 1000;

    private long processingTime;
    private long numEventsSinceLastWatermark;

    private ChildMerger childMerger;

    private boolean hasCountWindow;

    public DistributedChild(String parentIp, int parentControllerPort, int parentWindowPort, int streamInputPort, int childId, int numStreams) {
        this.nodeImpl = new DistributedNode(childId, NODE_IDENTIFIER, streamInputPort + STREAM_REGISTER_PORT_OFFSET,
                streamInputPort, numStreams, parentIp, parentControllerPort, parentWindowPort);
        this.hasCountWindow = false;

        nodeImpl.createDataPuller();
        nodeImpl.createWindowPusher(parentIp, parentWindowPort);
    }

    @Override
    public void run() {
        System.out.println(nodeImpl.nodeString("Starting child worker on port " + nodeImpl.dataPort +
                " with " + nodeImpl.numChildren + " stream(s). Connecting to parent at " +
                nodeImpl.parentIp + " with controller port " + nodeImpl.parentControllerPort +
                " and window port " + nodeImpl.parentWindowPort));

        try {
            WindowingConfig config = this.nodeImpl.registerAtParent();
            this.hasCountWindow = !config.getCountWindows().isEmpty();

            boolean registerSuccess = this.registerStreams(config);
            if (!registerSuccess) {
                return;
            }

            this.processStreams();
        } finally {
            nodeImpl.close();
        }
    }

    private void processStreams() {
        ZMQ.Socket streamInput = nodeImpl.dataPuller;
        System.out.println(nodeImpl.nodeString("Waiting for stream data."));

        currentEventTime = 0;
        lastWatermark = 0;
        numEvents = 0;
        processingTime = 0;
        numEventsSinceLastWatermark = 0;

        while (!nodeImpl.isInterrupted()) {
            String eventOrStreamEnd = streamInput.recvStr();
            if (eventOrStreamEnd == null) {
                continue;
            }

            if (eventOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                if (!nodeImpl.isTotalStreamEnd()) {
                    continue;
                }

                System.out.println(nodeImpl.nodeString("Processed " + numEvents + " events in total."));
                final long watermarkTimestamp = currentEventTime + nodeImpl.watermarkMs;
                handleWatermark(watermarkTimestamp);
                System.out.println(nodeImpl.nodeString("No more data to come. Ending child worker..."));
                nodeImpl.endChild();
                return;
            }

            if (this.hasCountWindow()) {
                nodeImpl.forwardEvent(eventOrStreamEnd);
            }

            this.processEvent(eventOrStreamEnd);
        }

        System.out.println(nodeImpl.nodeString("Interrupted while processing streams."));
    }

    private void processEvent(String eventString) {
//        final long processingStart = System.nanoTime();
        final Event event = Event.fromString(eventString);
        this.childMerger.processElement(event);
//        final long processingEnd = System.nanoTime();

        currentEventTime = event.getTimestamp();
        numEvents++;
        numEventsSinceLastWatermark++;
//        processingTime += (processingEnd - processingStart);

        // If we haven't processed a watermark in watermarkMs milliseconds and waited for the maximum lateness of a
        // tuple, process it.
        final long watermarkTimestamp = lastWatermark + nodeImpl.watermarkMs;
        if (currentEventTime >= watermarkTimestamp + MAX_LATENESS) {
            handleWatermark(watermarkTimestamp);
        }

    }

    private void handleWatermark(long watermarkTimestamp) {
        System.out.println("Processed " + numEventsSinceLastWatermark + " total events at watermark " + watermarkTimestamp);
//        System.out.println("Avg processing time: " + (processingTime / numEventsSinceLastWatermark) + " ns.");
        processingTime = 0;
        numEventsSinceLastWatermark = 0;

//        final long watermarkStart = System.nanoTime();
        List<DistributedAggregateWindowState> finalWindows =
                this.childMerger.processWatermarkedWindows(watermarkTimestamp);
//        final long watermarkEnd = System.nanoTime();
//        System.out.println("Watermark processing took " + (watermarkEnd - watermarkStart) + " ns.");

//        final long sendingStart = System.nanoTime();
        nodeImpl.sendPreAggregatedWindowsToParent(finalWindows);

        finalWindows.stream()
                .map(state -> childMerger.getNextSessionStart(state.getFunctionWindowId()))
                .forEach(newSession -> newSession.ifPresent(nodeImpl::sendSessionStartToParent));
        lastWatermark = watermarkTimestamp;
//        final long sendingEnd = System.nanoTime();
//        System.out.println("Watermark sending took " + (sendingEnd - sendingStart) + " ns.");
    }

    private boolean registerStreams(final WindowingConfig windowingConfig) {
        final ZMQ.Socket streamReceiver = nodeImpl.context.createSocket(SocketType.REP);
        streamReceiver.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        streamReceiver.bind(DistributedUtils.buildBindingTcpUrl(nodeImpl.dataPort + STREAM_REGISTER_PORT_OFFSET));

        String ackResponse = "ack";

        int numRegisteredStreams = 0;
        while (!nodeImpl.isInterrupted()) {
            final String rawStreamId = streamReceiver.recvStr();

            if (rawStreamId == null) {
                continue;
            }

            final int streamId = Integer.parseInt(rawStreamId);
            System.out.println(nodeImpl.nodeString("Registering stream " + streamId));
            streamReceiver.send(ackResponse);
            numRegisteredStreams++;

            if (numRegisteredStreams == nodeImpl.numChildren) {
                // All streams registered
                System.out.println(nodeImpl.nodeString("Registered all streams (" + numRegisteredStreams + " in total)"));
                this.childMerger = new ChildMerger(windowingConfig.getTimeWindows(),
                        windowingConfig.getAggregateFunctions(), nodeImpl.nodeId);
                return true;
            }
        }

        System.out.println(nodeImpl.nodeString("Interrupted while registering streams."));
        return false;
    }

    private boolean hasCountWindow() {
        return this.hasCountWindow;
    }

    public void interrupt() {
        nodeImpl.interrupt();
    }


}
