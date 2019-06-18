package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.NonDecomposableAggregateFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class DistributedChild implements Runnable {

    private final int childId;

    // Network related
    private final String rootIp;
    private final int rootControllerPort;
    private final int rootWindowPort;
    private final int streamInputPort;
    private final ZContext context;
    private boolean interrupt = false;

    private ZMQ.Socket windowPusher;
    public final static int STREAM_REGISTER_PORT_OFFSET = 100;
    public final static long STREAM_REGISTER_TIMEOUT_MS = 10 * 1000;

    // Slicing related
    private final Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;
    private final int numStreams;
    private DistributedWindowMerger<Integer> simpleStreamWindowMerger;
    private DistributedWindowMerger<AlgebraicPartial> algebraicStreamWindowMerger;
    private long watermarkMs;
    private Set<Integer> streamEnds;

    public DistributedChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamInputPort, int childId, int numStreams) {
        this.rootIp = rootIp;
        this.rootControllerPort = rootControllerPort;
        this.rootWindowPort = rootWindowPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;
        this.numStreams = numStreams;
        this.streamEnds = new HashSet<>(this.numStreams);
        this.slicerPerStream = new HashMap<>();
        this.context = new ZContext();
    }

    @Override
    public void run() {
        System.out.println(this.childIdString("Starting child worker on port " + this.streamInputPort +
                " with " + this.numStreams + " stream(s). Connecting to root at " + this.rootIp +
                " with controller port " + this.rootControllerPort + " and window port " + this.rootWindowPort));

        // 1. connect to root server
        // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
        WindowingConfig config = this.registerAtRoot();

        if (config.getWindows().isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any windows from root!"));
        }

        if (config.getAggregateFunctions().isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any aggFns from root!"));
        }

        // 2. register streams
        boolean registerSuccess = this.registerStreams(STREAM_REGISTER_TIMEOUT_MS, config);
        if (!registerSuccess) {
            return;
        }

        // 3. process streams
        // 4. send windows to root
        this.processStreams();
    }

    private boolean registerStreams(final long timeout, final WindowingConfig windowingConfig) {
        final ZMQ.Socket streamReceiver = this.context.createSocket(SocketType.REP);
        streamReceiver.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        streamReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.streamInputPort + STREAM_REGISTER_PORT_OFFSET));

        final MemoryStateFactory stateFactory = new MemoryStateFactory();
        byte[] ackResponse = new byte[] {'\0'};

        while (!interrupt) {
            final String rawStreamId = streamReceiver.recvStr();

            if (rawStreamId == null) {
                continue;
            }

            final int streamId = Integer.parseInt(rawStreamId);
            System.out.println(this.childIdString("Registering stream " + streamId));

            DistributedChildSlicer<Integer> childSlicer = new DistributedChildSlicer<>(stateFactory);
            for (Window window : windowingConfig.getWindows()) {
                childSlicer.addWindowAssigner(window);
            }

            for (AggregateFunction aggFn : windowingConfig.getAggregateFunctions()) {
                childSlicer.addWindowFunction(aggFn);
            }

            this.slicerPerStream.put(streamId, childSlicer);
            streamReceiver.send(ackResponse);

            if (this.slicerPerStream.size() == this.numStreams) {
                // All streams registered
                System.out.println(this.childIdString("Registered all streams (" + this.numStreams + " in total)"));
                this.simpleStreamWindowMerger = new DistributedWindowMerger<>(this.numStreams, windowingConfig.getWindows(),
                        windowingConfig.getAggregateFunctions());
                this.algebraicStreamWindowMerger = new DistributedWindowMerger<>(this.numStreams, windowingConfig.getWindows(),
                        windowingConfig.getAggregateFunctions());
                return true;
            }
        }

        System.out.println(this.childIdString("Interrupted while registering streams."));
        this.context.destroy();
        return false;
    }

    private void processStreams() {
        ZMQ.Socket streamInput = this.context.createSocket(SocketType.PULL);
        streamInput.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        streamInput.bind(DistributedUtils.buildBindingTcpUrl(this.streamInputPort));
        System.out.println(this.childIdString("Waiting for stream data."));

        long currentEventTime = 0;
        long lastWatermark = 0;
        long numEvents = 0;

        while (!interrupt) {
            String streamIdOrStreamEnd = streamInput.recvStr();
            if (streamIdOrStreamEnd == null) {
                continue;
            }

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

            final String[] eventParts = streamIdOrStreamEnd.split(",");
            final int streamId = Integer.parseInt(eventParts[0]);
            final long eventTimestamp = Long.valueOf(eventParts[1]);
            final int eventValue = Integer.valueOf(eventParts[2]);

            DistributedChildSlicer<Integer> perStreamSlicer = this.slicerPerStream.get(streamId);
            perStreamSlicer.processElement(perStreamSlicer.castFromObject(eventValue), eventTimestamp);
            currentEventTime = eventTimestamp;
            numEvents++;

            // If we haven't processed a watermark in watermarkMs milliseconds and waited for the maximum lateness of a
            // tuple, process it.
            final long maxLateness = this.watermarkMs;
            final long watermarkTimestamp = lastWatermark + this.watermarkMs;
            if (currentEventTime >= watermarkTimestamp + maxLateness) {
                // System.out.println(this.childIdString("Processing watermark " + watermarkTimestamp));
                this.processWatermarkedWindows(watermarkTimestamp);
                lastWatermark = watermarkTimestamp;
            }
        }

        System.out.println(this.childIdString("Interrupted while processing streams."));
        streamInput.setLinger(0);
        streamInput.close();
        this.context.destroy();
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
            WindowAggregateId windowId = preAggWindow.getWindowAggregateId();
            List<AggregateFunction> aggregateFunctions = preAggWindow.getAggregateFunctions();

            final List aggValues = preAggWindow.getAggValues();
            for (int i = 0; i < aggValues.size(); i++) {
                final AggregateFunction aggregateFunction = aggregateFunctions.get(i);
                final FunctionWindowAggregateId functionWindowId = new FunctionWindowAggregateId(windowId, i);

                final Optional<FunctionWindowAggregateId> triggerId;
                final DistributedWindowMerger currentMerger;

                // Get aggregate function and check type to select correct processing semantics
                // sum stays same, avg needs partial agg, median needs slice?
                if (aggregateFunction instanceof AlgebraicAggregateFunction) {
                    AlgebraicPartial partial = (AlgebraicPartial) aggValues.get(i);
                    triggerId = this.algebraicStreamWindowMerger.processPreAggregate(partial, functionWindowId);
                    currentMerger = this.algebraicStreamWindowMerger;
                } else if (aggregateFunction instanceof NonDecomposableAggregateFunction) {
                    // TODO: implement
                    throw new RuntimeException("NonDecomposable not supported.");
                } else {
                    // Simple aggregation with result merging
                    Integer partialAggregate = (Integer) aggValues.get(i);
                    triggerId = this.simpleStreamWindowMerger.processPreAggregate(partialAggregate, functionWindowId);
                    currentMerger = this.simpleStreamWindowMerger;
                }

                if (triggerId.isPresent()) {
                    AggregateWindow finalPreAggregateWindow = currentMerger.triggerFinalWindow(triggerId.get());
                    finalPreAggregateWindows.add(finalPreAggregateWindow);
                }
            }
        }

        return finalPreAggregateWindows;
    }


    private void sendPreAggregatedWindowsToRoot(List<AggregateWindow> preAggregatedWindows) {
        if (this.windowPusher == null) {
            this.windowPusher = this.context.createSocket(SocketType.PUSH);
            this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootWindowPort));
        }

        for (AggregateWindow<Integer> preAggregatedWindow : preAggregatedWindows) {
            WindowAggregateId windowId = preAggregatedWindow.getWindowAggregateId();

            // Convert partial aggregation result to byte array
            List<Integer> aggValues = preAggregatedWindow.getAggValues();
            Integer partialAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            String partialAggregateString = String.valueOf(partialAggregate);
            //  System.out.println(this.childIdString("Sending to root " + windowId));

            // Order:
            // Integer           childId
            // Integer,Long,Long WindowAggregateId
            // Byte[]            raw bytes of partial aggregate
            this.windowPusher.sendMore(String.valueOf(this.childId));
            this.windowPusher.sendMore(DistributedUtils.windowIdToString(windowId));
            this.windowPusher.send(partialAggregateString, ZMQ.DONTWAIT);
        }
    }

    private WindowingConfig registerAtRoot() {
        ZMQ.Socket controlClient = this.context.createSocket(SocketType.REQ);
        controlClient.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootControllerPort));

        controlClient.send(this.childIdString("I am a new child."));

        this.watermarkMs = Long.valueOf(controlClient.recvStr());
        String windowString = controlClient.recvStr();
        String aggString = controlClient.recvStr();
        System.out.println(this.childIdString("Received: " + this.watermarkMs +
                " ms watermark | [" + windowString.replace("\n", ";") + "] | [" + aggString.replace("\n", ";") + "]"));

        this.windowPusher = this.context.createSocket(SocketType.PUSH);
        this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootWindowPort));

        List<Window> windows = this.createWindowsFromString(windowString);
        List<AggregateFunction> aggregateFunctions = this.createAggFunctionsFromString(aggString);
        return new WindowingConfig(windows, aggregateFunctions);
    }

    private List<Window> createWindowsFromString(String windowString) {
        List<Window> windows = new ArrayList<>();

        String[] windowRows = windowString.split("\n");
        for (String windowRow : windowRows) {
            Window window = DistributedUtils.buildWindowFromString(windowRow);
            windows.add(window);
            System.out.println(this.childIdString("Adding window: " + window));
        }

        return windows;
    }

    private List<AggregateFunction> createAggFunctionsFromString(String aggFnString) {
        List<AggregateFunction> aggFns = new ArrayList<>();

        String[] aggFnRows = aggFnString.split("\n");
        for (String aggFnRow : aggFnRows) {
            AggregateFunction aggFn = DistributedUtils.buildAggregateFunctionFromString(aggFnRow);
            aggFns.add(aggFn);
            System.out.println(this.childIdString("Adding aggFn: " + aggFnRow));
        }

        return aggFns;
    }

    private String childIdString(String msg) {
        return "[CHILD-" + this.childId + "] " + msg;
    }

    private class WindowingConfig {
        private final List<Window> windows;
        private final List<AggregateFunction> aggregateFunctions;

        public WindowingConfig(List<Window> windows, List<AggregateFunction> aggregateFunctions) {
            this.windows = windows;
            this.aggregateFunctions = aggregateFunctions;
        }

        public List<Window> getWindows() {
            return windows;
        }

        public List<AggregateFunction> getAggregateFunctions() {
            return aggregateFunctions;
        }

    }

    public void interrupt() {
        this.interrupt = true;
    }
}
