package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.EVENT_STRING;
import static com.github.lawben.disco.DistributedUtils.NO_KEY;
import static com.github.lawben.disco.DistributedUtils.WINDOW_COMPLETE;
import static com.github.lawben.disco.DistributedUtils.WINDOW_PARTIAL;

import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import com.github.lawben.disco.aggregation.HolisticAggregateHelper;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
    private final int numStreams;
    private ChildMerger childMerger;
    private long watermarkMs;
    private Set<Integer> streamEnds;

    private boolean hasCountWindow;

    public DistributedChild(String rootIp, int rootControllerPort, int rootWindowPort, int streamInputPort, int childId, int numStreams) {
        this.rootIp = rootIp;
        this.rootControllerPort = rootControllerPort;
        this.rootWindowPort = rootWindowPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;
        this.numStreams = numStreams;
        this.streamEnds = new HashSet<>(this.numStreams);
        this.context = new ZContext();
        this.hasCountWindow = false;
    }

    @Override
    public void run() {
        System.out.println(this.childIdString("Starting child worker on port " + this.streamInputPort +
                " with " + this.numStreams + " stream(s). Connecting to root at " + this.rootIp +
                " with controller port " + this.rootControllerPort + " and window port " + this.rootWindowPort));

        // 1. connect to root server
        // 1.1. receive commands from root (i.e. windows, agg. functions, etc.)
        WindowingConfig config = this.registerAtRoot();

        if (config.getTimeWindows().isEmpty() && config.getCountWindows().isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any windows from root!"));
        }

        if (config.getAggregateFunctions().isEmpty()) {
            throw new RuntimeException(this.childIdString("Did not receive any aggFns from root!"));
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
    }

    private boolean registerStreams(final WindowingConfig windowingConfig) {
        final ZMQ.Socket streamReceiver = this.context.createSocket(SocketType.REP);
        streamReceiver.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        streamReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.streamInputPort + STREAM_REGISTER_PORT_OFFSET));

        byte[] ackResponse = new byte[] {'\0'};

        int numRegisteredStreams = 0;
        while (!interrupt) {
            final String rawStreamId = streamReceiver.recvStr();

            if (rawStreamId == null) {
                continue;
            }

            final int streamId = Integer.parseInt(rawStreamId);
            System.out.println(this.childIdString("Registering stream " + streamId));
            streamReceiver.send(ackResponse);
            numRegisteredStreams++;

            if (numRegisteredStreams == this.numStreams) {
                // All streams registered
                System.out.println(this.childIdString("Registered all streams (" + this.numStreams + " in total)"));
                this.childMerger = new ChildMerger(windowingConfig.timeWindows,
                        windowingConfig.getAggregateFunctions(), this.childId);
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
            String eventOrStreamEnd = streamInput.recvStr();
            if (eventOrStreamEnd == null) {
                continue;
            }

            if (eventOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                int streamId = Integer.valueOf(streamInput.recvStr(ZMQ.DONTWAIT));
                System.out.println(this.childIdString("Stream end from STREAM-" + streamId));
                this.streamEnds.add(streamId);
                if (this.streamEnds.size() == this.numStreams) {
                    System.out.println(this.childIdString("Processed " + numEvents + " events in total."));
                    final long watermarkTimestamp = currentEventTime + this.watermarkMs;
                    List<DistributedAggregateWindowState> finalWindows =
                            this.childMerger.processWatermarkedWindows(watermarkTimestamp);
                    this.sendPreAggregatedWindowsToRoot(finalWindows);
                    System.out.println(this.childIdString("No more data to come. Ending child worker..."));
                    this.windowPusher.sendMore(DistributedUtils.STREAM_END);
                    this.windowPusher.send(String.valueOf(this.childId));
                    return;
                }

                continue;
            }

            if (this.hasCountWindow()) {
                this.sendToRoot(Arrays.asList(EVENT_STRING, eventOrStreamEnd), this.windowPusher);
            }

            final String[] eventParts = eventOrStreamEnd.split(",");
            final int streamId = Integer.parseInt(eventParts[0]);
            final long eventTimestamp = Long.valueOf(eventParts[1]);
            final int eventValue = Integer.valueOf(eventParts[2]);
            final int key = eventParts.length == 4 ? Integer.valueOf(eventParts[3]) : NO_KEY;

            this.childMerger.processElement(eventValue, eventTimestamp, key);
            currentEventTime = eventTimestamp;
            numEvents++;

            // If we haven't processed a watermark in watermarkMs milliseconds and waited for the maximum lateness of a
            // tuple, process it.
            final long maxLateness = this.watermarkMs;
            final long watermarkTimestamp = lastWatermark + this.watermarkMs;
            if (currentEventTime >= watermarkTimestamp + maxLateness) {
                List<DistributedAggregateWindowState> finalWindows =
                        this.childMerger.processWatermarkedWindows(watermarkTimestamp);
                this.sendPreAggregatedWindowsToRoot(finalWindows);
                lastWatermark = watermarkTimestamp;
            }
        }

        System.out.println(this.childIdString("Interrupted while processing streams."));
        streamInput.setLinger(0);
        streamInput.close();
        this.context.destroy();
    }

    private void sendPreAggregatedWindowsToRoot(List<DistributedAggregateWindowState> preAggregatedWindows) {
        for (DistributedAggregateWindowState preAggregatedWindow : preAggregatedWindows) {
            List<String> serializedAggWindow = this.serializeAggregate(preAggregatedWindow);
            this.sendToRoot(serializedAggWindow, this.windowPusher);
        }
    }

    private List<String> serializeAggregate(DistributedAggregateWindowState aggWindow) {
        List<AggregateFunction> aggFns = aggWindow.getAggregateFunctions();
        if (aggFns.size() > 1) {
            throw new IllegalStateException("Final agg should only have one function.");
        }
        FunctionWindowAggregateId functionWindowAggId = aggWindow.getFunctionWindowId();

        List<String> serializedAgg = new ArrayList<>();
        // Add child id and window id for each aggregate type
        serializedAgg.add(String.valueOf(this.childId));
        serializedAgg.add(DistributedUtils.functionWindowIdToString(functionWindowAggId));
        serializedAgg.add(aggWindow.windowIsComplete() ? WINDOW_COMPLETE : WINDOW_PARTIAL);

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
        } else if (aggFn instanceof HolisticMergeWrapper) {
            serializedAgg.add(DistributedUtils.HOLISTIC_STRING);
            List<Slice> slices = (List<Slice>) aggValue;
            // Add functionId as slice might have multiple states
            serializedAgg.add(DistributedUtils.slicesToString(slices, functionWindowAggId.getFunctionId()));
        } else {
            throw new IllegalArgumentException("Unknown aggregate function type: " + aggFn.getClass().getSimpleName());
        }

        return serializedAgg;
    }

    private void sendToRoot(List<String> serializedMessage, ZMQ.Socket sender) {
        for (int i = 0; i < serializedMessage.size() - 1; i++) {
            sender.sendMore(serializedMessage.get(i));
        }
        sender.send(serializedMessage.get(serializedMessage.size() - 1), ZMQ.DONTWAIT);
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

        List<Window> allWindows = this.createWindowsFromString(windowString);
        List<AggregateFunction> aggregateFunctions = this.createAggFunctionsFromString(aggString);

        List<Window> timeWindows = allWindows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Time)
                .collect(Collectors.toList());

        List<Window> countWindows = allWindows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Count)
                .collect(Collectors.toList());

        return new WindowingConfig(timeWindows, countWindows, aggregateFunctions);
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

    private boolean hasCountWindow() {
        return this.hasCountWindow;
    }

    public void interrupt() {
        this.interrupt = true;
    }

    private class WindowingConfig {
        private final List<Window> timeWindows;
        private final List<Window> countWindows;
        private final List<AggregateFunction> aggregateFunctions;

        public WindowingConfig(List<Window> timeWindows, List<Window> countWindows, List<AggregateFunction> aggregateFunctions) {
            this.timeWindows = timeWindows;
            this.countWindows = countWindows;
            this.aggregateFunctions = aggregateFunctions;
        }

        public List<Window> getTimeWindows() {
            return timeWindows;
        }

        public List<Window> getCountWindows() {
            return countWindows;
        }

        public List<AggregateFunction> getAggregateFunctions() {
            return aggregateFunctions;
        }

    }
}
