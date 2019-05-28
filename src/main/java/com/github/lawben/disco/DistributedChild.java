package com.github.lawben.disco;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

public class DistributedChild implements Runnable {

    private final int childId;

    // Network related
    private final String rootIp;
    private final int rootControllerPort;
    private final int rootWindowPort;
    private final int streamInputPort;
    private final ZContext context;

    private ZMQ.Socket windowPusher;
    public final static int STREAM_REGISTER_PORT_OFFSET = 100;
    public final static long STREAM_REGISTER_TIMEOUT_MS = 10 * 1000;

    // Slicing related
    private final Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;
    private int numStreams;
    private DistributedWindowMerger<Integer> streamWindowMerger;

    private long watermarkMs;
    private Set<Integer> streamEnds;


    public DistributedChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamInputPort, int childId) {
        this.rootIp = rootIp;
        this.rootControllerPort = rootControllerPort;
        this.rootWindowPort = rootWindowPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;

        this.slicerPerStream = new HashMap<>();

        this.context = new ZContext();
    }

    @Override
    public void run() {
        System.out.println(this.childIdString("Starting child worker on port " + this.streamInputPort +
                ". Connecting to root at " + this.rootIp + " with controller port " + this.rootControllerPort +
                " and window port " + this.rootWindowPort));

        // 1. connect to root server
        // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
        List<Window> windows = this.registerAtRoot();

        if (windows.isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any windows from root!"));
        }

        // 2. register streams
        this.registerStreams(STREAM_REGISTER_TIMEOUT_MS, windows);
        this.streamEnds = new HashSet<>(this.numStreams);

        // 3. process streams
        // 4. send windows to root
        this.processStreams();

    }

    private void registerStreams(final long timeout, final List<Window> windows) {
        final ZMQ.Socket streamReceiver = this.context.createSocket(SocketType.REP);
        streamReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.streamInputPort + STREAM_REGISTER_PORT_OFFSET));

        final ZMQ.Poller streams = this.context.createPoller(1);
        streams.register(streamReceiver, Poller.POLLIN);

        final MemoryStateFactory stateFactory = new MemoryStateFactory();

        // TODO: also get this from root
        final ReduceAggregateFunction<Integer> aggFn = DistributedUtils.aggregateFunctionSum();
        byte[] ackResponse = new byte[] {'\0'};

        while (!Thread.currentThread().isInterrupted()) {
            if (streams.poll(timeout) == 0) {
                // Timed out --> all streams registered
                System.out.println(this.childIdString("Registered all streams."));
                this.streamWindowMerger = new DistributedWindowMerger<>(stateFactory, this.numStreams, windows, aggFn);
                return;
            }

            final int streamId = Integer.parseInt(streamReceiver.recvStr(ZMQ.DONTWAIT));
            System.out.println(this.childIdString("Registering stream " + streamId));

            DistributedChildSlicer<Integer> childSlicer = new DistributedChildSlicer<>(stateFactory);
            childSlicer.addWindowFunction(aggFn);
            for (Window window : windows) {
                childSlicer.addWindowAssigner(window);
            }
            this.slicerPerStream.put(streamId, childSlicer);
            this.numStreams++;

            streamReceiver.send(ackResponse);
        }
    }

    private void processStreams() {
        ZMQ.Socket streamInput = this.context.createSocket(SocketType.PULL);
        streamInput.bind(DistributedUtils.buildBindingTcpUrl(this.streamInputPort));
        System.out.println(this.childIdString("Waiting for stream data."));

        long currentEventTime = 0;
        long lastWatermark = 0;
        long numEvents = 0;

        while (!Thread.currentThread().isInterrupted()) {
            String streamIdOrStreamEnd = streamInput.recvStr();
            if (streamIdOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                int streamId = Integer.valueOf(streamInput.recvStr(ZMQ.DONTWAIT));
                System.out.println(this.childIdString("Stream end from STREAM-" + streamId));
                this.streamEnds.add(streamId);
                if (this.streamEnds.size() == this.numStreams) {
                    System.out.println(this.childIdString("Processed " + numEvents + " events in total."));
                    final long watermarkTimestamp = currentEventTime + this.watermarkMs;
                    this.processWatermarkedWindows(watermarkTimestamp);
                    System.out.println(this.childIdString("No more data to come. Ending child worker..."));
                    this.windowPusher.sendMore(DistributedUtils.STREAM_END);
                    this.windowPusher.send(String.valueOf(this.childId));
                    return;
                }

                continue;
            }

            String[] eventParts = streamIdOrStreamEnd.split(",");
            int streamId = Integer.parseInt(eventParts[0]);
            long eventTimestamp = Long.valueOf(eventParts[1]);
            int eventValue = Integer.valueOf(eventParts[2]);

            DistributedChildSlicer<Integer> perStreamSlicer = this.slicerPerStream.get(streamId);
            perStreamSlicer.processElement(perStreamSlicer.castFromObject(eventValue), eventTimestamp);
            currentEventTime = eventTimestamp;
            numEvents++;


            // If we haven't processed a watermark in watermarkMs milliseconds, process it.
            final long watermarkTimestamp = lastWatermark + this.watermarkMs;
            if (currentEventTime >= watermarkTimestamp) {
                // System.out.println(this.childIdString("Processing watermark " + watermarkTimestamp));
                this.processWatermarkedWindows(watermarkTimestamp);
                lastWatermark = watermarkTimestamp;
            }
        }
    }

    private void processWatermarkedWindows(long watermarkTimestamp) {
        this.slicerPerStream.forEach((streamId, slicer) -> {
            List<AggregateWindow> preAggregatedWindows = slicer.processWatermark(watermarkTimestamp);
            List<AggregateWindow> finalPreAggregateWindows = this.mergeStreamWindows(preAggregatedWindows);
            this.sendPreAggregatedWindowsToRoot(finalPreAggregateWindows);
        });
    }

    private List<AggregateWindow> mergeStreamWindows(List<AggregateWindow> preAggregatedWindows) {
        List<AggregateWindow> finalPreAggregateWindows = new ArrayList<>(preAggregatedWindows.size());

        for (AggregateWindow preAggWindow : preAggregatedWindows) {
            AggregateWindow<Integer> typedPreAggregateWindow = (AggregateWindow<Integer>) preAggWindow;
            List<Integer> aggValues = typedPreAggregateWindow.getAggValues();
            Integer partialAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            WindowAggregateId windowId = preAggWindow.getWindowAggregateId();
            boolean finalTrigger = this.streamWindowMerger.processPreAggregate(partialAggregate, windowId);

            if (finalTrigger) {
                AggregateWindow<Integer> finalPreAggregateWindow = this.streamWindowMerger.triggerFinalWindow(windowId);
                finalPreAggregateWindows.add(finalPreAggregateWindow);
            }
        }

        return finalPreAggregateWindows;
    }


    private void sendPreAggregatedWindowsToRoot(List<AggregateWindow> preAggregatedWindows) {
        if (this.windowPusher == null) {
            this.windowPusher = this.context.createSocket(SocketType.PUSH);
            this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootWindowPort));
        }

        for (AggregateWindow preAggregatedWindow : preAggregatedWindows) {
            WindowAggregateId windowId = preAggregatedWindow.getWindowAggregateId();

            // Convert partial aggregation result to byte array
            List aggValues = preAggregatedWindow.getAggValues();
            Object partialAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            byte[] partialAggregateBytes = DistributedUtils.objectToBytes(partialAggregate);
            //  System.out.println(this.childIdString("Sending to root " + windowId));

            // Order:
            // Integer           childId
            // Integer,Long,Long WindowAggregateId
            // Byte[]            raw bytes of partial aggregate
            this.windowPusher.sendMore(String.valueOf(this.childId));
            this.windowPusher.sendMore(DistributedUtils.windowIdToString(windowId));
            this.windowPusher.send(partialAggregateBytes, ZMQ.DONTWAIT);
        }
    }

    private List<Window> registerAtRoot() {
        ZMQ.Socket controlClient = this.context.createSocket(SocketType.REQ);
        controlClient.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootControllerPort));

        controlClient.send(this.childIdString("I am a new child."));

        this.watermarkMs = Long.valueOf(controlClient.recvStr());
        String windowString = controlClient.recvStr();
        System.out.println(this.childIdString("Received: " + this.watermarkMs +
                " [" + windowString.replace("\n", ";") + "]"));

        this.windowPusher = this.context.createSocket(SocketType.PUSH);
        this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootWindowPort));

        return this.createWindowsFromString(windowString);
    }

    private List<Window> createWindowsFromString(String windowString) {
        ArrayList<Window> windows = new ArrayList<>();

        String[] windowRows = windowString.split("\n");
        for (String windowRow : windowRows) {
            windows.add(DistributedUtils.buildWindowFromString(windowRow));
            System.out.println(this.childIdString("Adding window: " + windows.get(windows.size() - 1)));
        }

        return windows;
    }

    private String childIdString(String msg) {
        return "[CHILD-" + this.childId + "] " + msg;
    }
}
