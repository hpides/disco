package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.WINDOW_COMPLETE;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class DistributedRoot implements Runnable {
    // Network related
    private final ZContext context;
    private final int controllerPort;
    private final int windowPort;
    private final String resultPath;
    private ZMQ.Socket windowPuller;
    private ZMQ.Socket resultPusher;
    private final int numChildren;
    private Set<Integer> childStreamEnds;

    private final String[] windowStrings;
    private final String[] aggregateFnStrings;

    private long watermarkMs;

    private boolean interrupt;

    // Slicing related
    private DistributiveWindowMerger<Integer> distributiveWindowMerger;
    private AlgebraicWindowMerger<AlgebraicPartial> algebraicWindowMerger;
    private GlobalHolisticWindowMerger holisticWindowMerger;
    private DistributedChildSlicer<Integer> countBasedSlicer;

    public DistributedRoot(int controllerPort, int windowPort, String resultPath, int numChildren, String windowsString, String aggregateFunctionsString) {
        this.controllerPort = controllerPort;
        this.windowPort = windowPort;
        this.resultPath = resultPath;
        this.numChildren = numChildren;
        this.windowStrings = windowsString.split(";");
        this.aggregateFnStrings = aggregateFunctionsString.split(";");

        if (this.windowStrings.length == 0) {
            throw new IllegalArgumentException("Need at least one window.");
        }

        if (this.aggregateFnStrings.length == 0) {
            throw new IllegalArgumentException("Need at least one aggregate function.");
        }

        this.interrupt = false;

        this.childStreamEnds = new HashSet<>(this.numChildren);

        this.context = new ZContext();
        this.windowPuller = this.context.createSocket(SocketType.PULL);
        this.windowPuller.setReceiveTimeOut(DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS);
        this.windowPuller.bind(DistributedUtils.buildBindingTcpUrl(this.windowPort));
        this.resultPusher = this.context.createSocket(SocketType.PUSH);
        this.resultPusher.connect(DistributedUtils.buildIpcUrl(this.resultPath));
    }

    @Override
    public void run() {
        System.out.println(this.rootString("Starting root worker with controller port " + this.controllerPort +
                ", window port " + this.windowPort + " and result path " + this.resultPath));

        this.waitForChildren(this.numChildren);
        this.processPreAggregatedWindows();
    }

    private void processPreAggregatedWindows() {
        long currentEventTime = 0;
        long lastWatermark = 0;
        long numEvents = 0;

        while (!this.interrupt) {
            String messageOrStreamEnd = this.windowPuller.recvStr();
            if (messageOrStreamEnd == null) {
                continue;
            }

            if (messageOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                int childId = Integer.valueOf(this.windowPuller.recvStr(ZMQ.DONTWAIT));
                System.out.println(this.rootString("Stream end from CHILD-" + childId));
                this.childStreamEnds.add(childId);

                // TODO: fix!
                //this.distributiveWindowMerger.removeChild();

                if (this.childStreamEnds.size() == this.numChildren) {
                    System.out.println(this.rootString("Received all stream ends. Shutting down root..."));
                    this.resultPusher.send(DistributedUtils.STREAM_END);
                    return;
                }

                continue;
            }

//            if (messageOrStreamEnd.equals(DistributedUtils.EVENT_STRING)) {
//                String rawEvent = this.windowPuller.recvStr(ZMQ.DONTWAIT);
//                final String[] eventParts = rawEvent.split(",");
//                final int streamId = Integer.parseInt(eventParts[0]);
//                final long eventTimestamp = Long.valueOf(eventParts[1]);
//                final int eventValue = Integer.valueOf(eventParts[2]);
//                this.countBasedSlicer.processElement(eventValue, eventTimestamp);
//
//                currentEventTime = eventTimestamp;
//                numEvents++;
//                final long maxLateness = this.watermarkMs;
//                final long watermarkTimestamp = lastWatermark + this.watermarkMs;
//                if (currentEventTime >= watermarkTimestamp + maxLateness) {
//                    this.processWatermarkedWindows(watermarkTimestamp);
//                    lastWatermark = watermarkTimestamp;
//                }
//
//                continue;
//            }

            int childId = Integer.valueOf(messageOrStreamEnd);
            String rawFunctionWindowAggId = this.windowPuller.recvStr(ZMQ.DONTWAIT);
            String aggregateType = this.windowPuller.recvStr(ZMQ.DONTWAIT);
            boolean windowIsComplete = this.windowPuller.recvStr(ZMQ.DONTWAIT).equals(WINDOW_COMPLETE);
            String rawPreAggregate = this.windowPuller.recvStr(ZMQ.DONTWAIT);

            FunctionWindowAggregateId functionWindowAggId =
                    DistributedUtils.stringToFunctionWindowAggId(rawFunctionWindowAggId);

            this.processPreAggregateWindow(functionWindowAggId, aggregateType, rawPreAggregate, windowIsComplete);
        }
    }

    private void processPreAggregateWindow(FunctionWindowAggregateId functionWindowId, String aggregateType, String rawPreAggregate, boolean windowIsComplete) {
        final Optional<FunctionWindowAggregateId> triggerId;
        final WindowMerger currentMerger;

        switch (aggregateType) {
            case DistributedUtils.DISTRIBUTIVE_STRING:
                Integer partialAggregate = Integer.valueOf(rawPreAggregate);
                triggerId = this.distributiveWindowMerger.processPreAggregate(partialAggregate, functionWindowId);
                currentMerger = this.distributiveWindowMerger;
                break;
            case DistributedUtils.ALGEBRAIC_STRING:
                List<AggregateFunction> algebraicFns = this.algebraicWindowMerger.getAggregateFunctions();
                AlgebraicMergeFunction algebraicMergeFn = (AlgebraicMergeFunction) algebraicFns.get(functionWindowId.getFunctionId());
                AlgebraicAggregateFunction algebraicFn = algebraicMergeFn.getOriginalFn();
                AlgebraicPartial partial = algebraicFn.partialFromString(rawPreAggregate);
                triggerId = this.algebraicWindowMerger.processPreAggregate(partial, functionWindowId);
                currentMerger = this.algebraicWindowMerger;
                break;
            case DistributedUtils.HOLISTIC_STRING:
                List<DistributedSlice> slices = DistributedUtils.slicesFromString(rawPreAggregate);
                triggerId = this.holisticWindowMerger.processPreAggregateAndCheckComplete(slices, functionWindowId, windowIsComplete);
                currentMerger = this.holisticWindowMerger;
                break;
            default:
                throw new IllegalArgumentException("Unknown aggregate type: " + aggregateType);
        }

        if (triggerId.isEmpty()) {
            return;
        }

        FunctionWindowAggregateId finalWindowId = triggerId.get();
        AggregateWindow finalWindow = currentMerger.triggerFinalWindow(finalWindowId);
        Integer finalAggregate = currentMerger.lowerFinalValue(finalWindow);
        String finalAggregateString = String.valueOf(finalAggregate);

        this.resultPusher.sendMore(DistributedUtils.functionWindowIdToString(finalWindowId));
        this.resultPusher.send(finalAggregateString, ZMQ.DONTWAIT);
    }

    private void waitForChildren(int numChildren) {
        ZMQ.Socket childReceiver = this.context.createSocket(SocketType.REP);
        childReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.controllerPort));

        List<Window> windows = Arrays.stream(this.windowStrings)
                .map(DistributedUtils::buildWindowFromString)
                .collect(Collectors.toList());

        List<Long> sessionWatermarkMs = windows.stream()
                .filter(w -> w instanceof SessionWindow)
                .map(w -> ((SessionWindow) w).getGap())
                .collect(Collectors.toList());

        List<Long> tumblingWatermarkMs = windows.stream()
                .filter(w -> w instanceof TumblingWindow)
                .map(w -> ((TumblingWindow) w).getSize())
                .collect(Collectors.toList());

        List<Long> slidingWatermarkMs = windows.stream()
                .filter(w -> w instanceof SlidingWindow)
                .map(w -> ((SlidingWindow) w).getSize())
                .collect(Collectors.toList());

        this.watermarkMs = Stream.of(sessionWatermarkMs, slidingWatermarkMs, tumblingWatermarkMs)
                .flatMap(Collection::stream)
                .min(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalArgumentException("Could not find watermark ms."));

        List<AggregateFunction> aggFn = Arrays.stream(this.aggregateFnStrings)
                .map(DistributedUtils::buildAggregateFunctionFromString)
                .collect(Collectors.toList());

        // Set up root the same way as the children will be set up.
        setupWindowMerger(windows, aggFn);

        String completeWindowString = String.join("\n", this.windowStrings);
        String completeAggFnString = String.join("\n", this.aggregateFnStrings);
        int numChildrenRegistered = 0;
        while (numChildrenRegistered < numChildren) {
            String message = childReceiver.recvStr();
            System.out.println(this.rootString("Received from child: " + message));

            childReceiver.sendMore(String.valueOf(this.watermarkMs));
            childReceiver.sendMore(completeWindowString);
            childReceiver.send(completeAggFnString);
            numChildrenRegistered++;
        }
    }

    public void setupWindowMerger(List<Window> windows, List<AggregateFunction> aggFns) {
        List<AggregateFunction> stateAggFunctions = DistributedUtils.convertAggregateFunctions(aggFns);

        this.distributiveWindowMerger = new DistributiveWindowMerger<>(this.numChildren, windows, stateAggFunctions);
        this.algebraicWindowMerger = new AlgebraicWindowMerger<>(this.numChildren, windows, stateAggFunctions);
        this.holisticWindowMerger = new GlobalHolisticWindowMerger(this.numChildren, windows, stateAggFunctions);

        this.countBasedSlicer = new DistributedChildSlicer<>(windows, aggFns);
    }

    private String rootString(String msg) {
        return "[ROOT] " + msg;
    }

    public void interrupt() {
        this.interrupt = true;
    }
}
