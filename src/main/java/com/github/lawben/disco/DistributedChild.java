package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.EVENT_STRING;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.merge.ChildMerger;
import java.util.Arrays;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class DistributedChild implements Runnable {
    private final static String NODE_IDENTIFIER = "CHILD";

    private final DistributedChildNode childNodeImpl;
    private final DistributedParentNode parentNodeImpl;

    private long currentEventTime;
    private long lastWatermark;
    private long numEvents;

    private ZMQ.Socket windowPusher;
    public final static int STREAM_REGISTER_PORT_OFFSET = 100;
    public final static long STREAM_REGISTER_TIMEOUT_MS = 10 * 1000;

    // Slicing related
    private ChildMerger childMerger;

    private boolean hasCountWindow;

    public DistributedChild(String parentIp, int parentControllerPort, int parentWindowPort, int streamInputPort, int childId, int numStreams) {
        this.childNodeImpl = new DistributedChildNode(childId, NODE_IDENTIFIER, parentIp, parentControllerPort, parentWindowPort);
        this.parentNodeImpl = new DistributedParentNode(childId, NODE_IDENTIFIER,
                streamInputPort + STREAM_REGISTER_PORT_OFFSET, streamInputPort, numStreams);
        this.hasCountWindow = false;

        parentNodeImpl.createDataPuller();
        parentNodeImpl.createWindowPusher(parentIp, parentWindowPort);
    }

    @Override
    public void run() {
        System.out.println(childNodeImpl.nodeString("Starting child worker on port " + parentNodeImpl.dataPort +
                " with " + parentNodeImpl.numChildren + " stream(s). Connecting to root at " + childNodeImpl.parentIp +
                " with controller port " + childNodeImpl.parentControllerPort + " and window port " +
                childNodeImpl.parentWindowPort));

        try {
            // 1. connect to root server
            // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
            WindowingConfig config = this.childNodeImpl.registerAtParent();

            if (config.getTimeWindows().isEmpty() && config.getCountWindows().isEmpty()) {
                throw new RuntimeException(childNodeImpl.nodeString("Did not receive any windows from root!"));
            }

            if (config.getAggregateFunctions().isEmpty()) {
                throw new RuntimeException(childNodeImpl.nodeString("Did not receive any aggFns from root!"));
            }

            this.hasCountWindow = !config.getCountWindows().isEmpty();

            // 2. register streams
            boolean registerSuccess = this.registerStreams(config);
            if (!registerSuccess) {
                return;
            }

            // 3. process streams
            // 4. send windows to root
            this.processStreams();
        } finally {
            parentNodeImpl.close();
            childNodeImpl.close();
        }
    }

    private void processStreams() {
        ZMQ.Socket streamInput = parentNodeImpl.dataPuller;
        System.out.println(childNodeImpl.nodeString("Waiting for stream data."));

        currentEventTime = 0;
        lastWatermark = 0;
        numEvents = 0;

        while (!parentNodeImpl.isInterrupted()) {
            String eventOrStreamEnd = streamInput.recvStr();
            if (eventOrStreamEnd == null) {
                continue;
            }

            if (eventOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                if (!parentNodeImpl.isTotalStreamEnd()) {
                    continue;
                }

                System.out.println(parentNodeImpl.nodeString("Processed " + numEvents + " events in total."));
                final long watermarkTimestamp = currentEventTime + childNodeImpl.watermarkMs;
                List<DistributedAggregateWindowState> finalWindows =
                        this.childMerger.processWatermarkedWindows(watermarkTimestamp);
                parentNodeImpl.sendPreAggregatedWindowsToParent(finalWindows);
                System.out.println(childNodeImpl.nodeString("No more data to come. Ending child worker..."));
                parentNodeImpl.endChild();
                return;
            }

            if (this.hasCountWindow()) {
                parentNodeImpl.sendToParent(Arrays.asList(EVENT_STRING, eventOrStreamEnd));
            }

            this.processEvent(eventOrStreamEnd);
        }

        System.out.println(childNodeImpl.nodeString("Interrupted while processing streams."));
    }

    private void processEvent(String eventString) {
        final Event event = Event.fromString(eventString);
        this.childMerger.processElement(event);
        currentEventTime = event.getTimestamp();
        numEvents++;

        // If we haven't processed a watermark in watermarkMs milliseconds and waited for the maximum lateness of a
        // tuple, process it.
        final long maxLateness = childNodeImpl.watermarkMs;
        final long watermarkTimestamp = lastWatermark + childNodeImpl.watermarkMs;
        if (currentEventTime >= watermarkTimestamp + maxLateness) {
            List<DistributedAggregateWindowState> finalWindows =
                    this.childMerger.processWatermarkedWindows(watermarkTimestamp);
            parentNodeImpl.sendPreAggregatedWindowsToParent(finalWindows);
            lastWatermark = watermarkTimestamp;
        }
    }

    private boolean registerStreams(final WindowingConfig windowingConfig) {
        final ZMQ.Socket streamReceiver = parentNodeImpl.context.createSocket(SocketType.REP);
        streamReceiver.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        streamReceiver.bind(DistributedUtils.buildBindingTcpUrl(parentNodeImpl.dataPort + STREAM_REGISTER_PORT_OFFSET));

        byte[] ackResponse = new byte[] {'\0'};

        int numRegisteredStreams = 0;
        while (!parentNodeImpl.isInterrupted()) {
            final String rawStreamId = streamReceiver.recvStr();

            if (rawStreamId == null) {
                continue;
            }

            final int streamId = Integer.parseInt(rawStreamId);
            System.out.println(childNodeImpl.nodeString("Registering stream " + streamId));
            streamReceiver.send(ackResponse);
            numRegisteredStreams++;

            if (numRegisteredStreams == parentNodeImpl.numChildren) {
                // All streams registered
                System.out.println(childNodeImpl.nodeString("Registered all streams (" + numRegisteredStreams + " in total)"));
                this.childMerger = new ChildMerger(windowingConfig.getTimeWindows(),
                        windowingConfig.getAggregateFunctions(), parentNodeImpl.nodeId);
                return true;
            }
        }

        System.out.println(childNodeImpl.nodeString("Interrupted while registering streams."));
        return false;
    }

    private boolean hasCountWindow() {
        return this.hasCountWindow;
    }

    public void interrupt() {
        parentNodeImpl.interrupt();
    }


}
