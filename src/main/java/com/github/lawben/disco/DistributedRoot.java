package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
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

    private boolean interrupt;

    // Slicing related
    private DistributiveWindowMerger<Integer> distributiveWindowMerger;
    private AlgebraicWindowMerger<AlgebraicPartial> algebraicWindowMerger;
    private LocalHolisticWindowMerger holisticWindowMerger;

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
        while (!this.interrupt) {
            String childIdOrStreamEnd = this.windowPuller.recvStr();
            if (childIdOrStreamEnd == null) {
                continue;
            }

            if (childIdOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
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

            int childId = Integer.valueOf(childIdOrStreamEnd);
            String rawAggregateWindowId = this.windowPuller.recvStr(ZMQ.DONTWAIT);
            String aggregateType = this.windowPuller.recvStr(ZMQ.DONTWAIT);
            String rawPreAggregate = this.windowPuller.recvStr(ZMQ.DONTWAIT);

            WindowAggregateId windowId = DistributedUtils.stringToWindowId(rawAggregateWindowId);

            // TODO: fix!
            FunctionWindowAggregateId functionWindowAggId = new FunctionWindowAggregateId(windowId, 0);

            this.processPreAggregateWindow(functionWindowAggId, aggregateType, rawPreAggregate);
        }
    }

    private void processPreAggregateWindow(FunctionWindowAggregateId functionWindowId, String aggregateType, String rawPreAggregate) {
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
                throw new RuntimeException("holistic not supported");
            default:
                throw new IllegalArgumentException("Unknown aggregate type: " + aggregateType);
        }

        if (triggerId.isEmpty()) {
            return;
        }

        AggregateWindow finalWindow = currentMerger.triggerFinalWindow(triggerId.get());
        Integer finalAggregate = currentMerger.lowerFinalValue(finalWindow);
        String finalAggregateString = String.valueOf(finalAggregate);

        this.resultPusher.sendMore(DistributedUtils.windowIdToString(functionWindowId.getWindowId()));
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
                .map(w -> ((SlidingWindow) w).getSlide())
                .collect(Collectors.toList());

        long watermarkMs = Stream.of(sessionWatermarkMs, slidingWatermarkMs, tumblingWatermarkMs)
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

            childReceiver.sendMore(String.valueOf(watermarkMs));
            childReceiver.sendMore(completeWindowString);
            childReceiver.send(completeAggFnString);
            numChildrenRegistered++;
        }
    }

    public void setupWindowMerger(List<Window> windows, List<AggregateFunction> aggFns) {
        List<AggregateFunction> distributiveFunctions = aggFns.stream()
                .filter((fn) -> fn instanceof DistributiveAggregateFunction)
                .collect(Collectors.toList());

        List<AggregateFunction> algebraicFunctions = DistributedUtils.convertAlgebraicFunctions(aggFns);

        this.distributiveWindowMerger = new DistributiveWindowMerger<>(this.numChildren, windows, distributiveFunctions);
        this.algebraicWindowMerger = new AlgebraicWindowMerger<>(this.numChildren, windows, algebraicFunctions);
        this.holisticWindowMerger = new LocalHolisticWindowMerger();
    }

    private String rootString(String msg) {
        return "[ROOT] " + msg;
    }

    public void interrupt() {
        this.interrupt = true;
    }
}
