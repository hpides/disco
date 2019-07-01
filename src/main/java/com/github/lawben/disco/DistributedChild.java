package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import com.github.lawben.disco.aggregation.HolisticAggregateWrapper;
import com.github.lawben.disco.aggregation.HolisticDummyFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateWindowState;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
    private DistributiveWindowMerger<Integer> distributiveWindowMerger;
    private AlgebraicWindowMerger<AlgebraicPartial> algebraicWindowMerger;
    private HolisticWindowMerger holisticWindowMerger;
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
        boolean registerSuccess = this.registerStreams(config);
        if (!registerSuccess) {
            return;
        }

        // 3. process streams
        // 4. send windows to root
        this.processStreams();
    }

    private boolean registerStreams(final WindowingConfig windowingConfig) {
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
                if (aggFn instanceof HolisticAggregateFunction) {
                    AggregateFunction wrapperAggFn = new HolisticAggregateWrapper();
                    childSlicer.addWindowFunction(wrapperAggFn);
                } else {
                    childSlicer.addWindowFunction(aggFn);
                }
            }

            this.slicerPerStream.put(streamId, childSlicer);
            streamReceiver.send(ackResponse);

            if (this.slicerPerStream.size() == this.numStreams) {
                // All streams registered
                System.out.println(this.childIdString("Registered all streams (" + this.numStreams + " in total)"));
                createWindowMergers(windowingConfig);
                return true;
            }
        }

        System.out.println(this.childIdString("Interrupted while registering streams."));
        this.context.destroy();
        return false;
    }

    private void createWindowMergers(WindowingConfig windowingConfig) {
        List<AggregateFunction> functions = windowingConfig.getAggregateFunctions();
        List<AggregateFunction> distributiveFunctions = functions.stream()
                .filter((fn) -> fn instanceof DistributiveAggregateFunction)
                .collect(Collectors.toList());

        List<AggregateFunction> algebraicFunctions = functions.stream()
                .filter((fn) -> fn instanceof AlgebraicAggregateFunction)
                .map(x -> new AlgebraicMergeFunction())
                .collect(Collectors.toList());

        List<Window> windows = windowingConfig.getWindows();
        this.distributiveWindowMerger = new DistributiveWindowMerger<>(this.numStreams, windows, distributiveFunctions);
        this.algebraicWindowMerger = new AlgebraicWindowMerger<>(this.numStreams, windows, algebraicFunctions);
        this.holisticWindowMerger = new HolisticWindowMerger();
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
            List<AggregateWindow> finalPreAggregateWindows = this.mergeStreamWindows(preAggregatedWindows, streamId);
            this.sendPreAggregatedWindowsToRoot(finalPreAggregateWindows);
        });
    }

    private List<AggregateWindow> mergeStreamWindows(List<AggregateWindow> preAggregatedWindows, int streamId) {
        List<AggregateWindow> finalPreAggregateWindows = new ArrayList<>(preAggregatedWindows.size());

        preAggregatedWindows.sort(Comparator.comparingLong(AggregateWindow::getStart));
        for (AggregateWindow preAggWindow : preAggregatedWindows) {
            List<AggregateWindow> aggregateWindows = mergePreAggWindow((AggregateWindowState) preAggWindow, streamId);
            finalPreAggregateWindows.addAll(aggregateWindows);
        }

        return finalPreAggregateWindows;
    }

    private List<AggregateWindow> mergePreAggWindow(AggregateWindowState preAggWindow, int streamId) {
        List<AggregateWindow> finalPreAggregateWindows = new ArrayList<>();
        WindowAggregateId windowId = preAggWindow.getWindowAggregateId();
        List<AggregateFunction> aggregateFunctions = preAggWindow.getAggregateFunctions();

        int numDistributiveFns = 0;
        int numAlgebraicFns = 0;

        final List aggValues = preAggWindow.getAggValues();
        for (int functionId = 0; functionId < aggValues.size(); functionId++) {
            final AggregateFunction aggregateFunction = aggregateFunctions.get(functionId);
            FunctionWindowAggregateId functionWindowId = new FunctionWindowAggregateId(windowId, functionId, streamId);

            final Optional<FunctionWindowAggregateId> triggerId;
            final WindowMerger currentMerger;

            if (aggregateFunction instanceof AlgebraicAggregateFunction) {
                functionWindowId = new FunctionWindowAggregateId(windowId, numAlgebraicFns++, streamId);
                AlgebraicPartial partial = (AlgebraicPartial) aggValues.get(functionId);
                triggerId = this.algebraicWindowMerger.processPreAggregate(partial, functionWindowId);
                currentMerger = this.algebraicWindowMerger;
            } else if (aggregateFunction instanceof HolisticAggregateWrapper) {
                List<Slice> slices = preAggWindow.getSlices();
                triggerId = this.holisticWindowMerger.processPreAggregate(slices, functionWindowId);
                currentMerger = this.holisticWindowMerger;
            } else if (aggregateFunction instanceof DistributiveAggregateFunction) {
                functionWindowId = new FunctionWindowAggregateId(windowId, numDistributiveFns++, streamId);
                Integer partialAggregate = (Integer) aggValues.get(functionId);
                triggerId = this.distributiveWindowMerger.processPreAggregate(partialAggregate, functionWindowId);
                currentMerger = this.distributiveWindowMerger;
            } else {
                throw new RuntimeException("Unsupported aggregate function: " + aggregateFunction);
            }

            if (triggerId.isPresent()) {
                AggregateWindow finalPreAggregateWindow = currentMerger.triggerFinalWindow(triggerId.get());
                finalPreAggregateWindows.add(finalPreAggregateWindow);
            }
        }

        return finalPreAggregateWindows;
    }


    private void sendPreAggregatedWindowsToRoot(List<AggregateWindow> preAggregatedWindows) {
        for (AggregateWindow preAggregatedWindow : preAggregatedWindows) {
            List<String> serializedAggWindow = this.serializeAggregate(preAggregatedWindow);
            this.sendToRoot(serializedAggWindow, this.windowPusher);
        }
    }

    private List<String> serializeAggregate(AggregateWindow aggWindow) {
        List<AggregateFunction> aggFns = aggWindow.getAggregateFunctions();
        if (aggFns.size() > 1) {
            throw new IllegalStateException("Final agg should only have one function.");
        }
        WindowAggregateId windowId = aggWindow.getWindowAggregateId();

        List<String> serializedAgg = new ArrayList<>();
        // Add child id and window id for each aggregate type
        serializedAgg.add(String.valueOf(this.childId));
        serializedAgg.add(DistributedUtils.windowIdToString(windowId));

        List aggValues = aggWindow.getAggValues();
        if (aggValues.isEmpty()) {
            serializedAgg.add(null);
            return serializedAgg;
        }

        AggregateFunction aggFn = aggFns.get(0);
        Object aggValue = aggValues.get(0);
        if (aggFn instanceof DistributiveAggregateFunction) {
            serializedAgg.add(DistributedUtils.DISTRIBUTIVE_STRING);
            Integer partialAggregate = (Integer) aggValue;
            String partialAggregateString = String.valueOf(partialAggregate);
            serializedAgg.add(partialAggregateString);
        } else if (aggFn instanceof AlgebraicMergeFunction) {
            serializedAgg.add(DistributedUtils.ALGEBRAIC_STRING);
            AlgebraicPartial partial = (AlgebraicPartial) aggValue;
            serializedAgg.add(partial.asString());
        } else if (aggFn instanceof HolisticDummyFunction) {
            serializedAgg.add(DistributedUtils.HOLISTIC_STRING);
            List<Slice> slices = (List<Slice>) aggValue;
            serializedAgg.add(DistributedUtils.slicesToString(slices));
        } else {
            throw new IllegalArgumentException("Unknown aggregate function type: " + aggFn.getClass().getSimpleName());
        }

        return serializedAgg;
    }

    private void sendToRoot(List<String> serializedAggWindow, ZMQ.Socket sender) {
        for (int i = 0; i < serializedAggWindow.size() - 1; i++) {
            sender.sendMore(serializedAggWindow.get(i));
        }
        sender.send(serializedAggWindow.get(serializedAggWindow.size() - 1), ZMQ.DONTWAIT);
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
