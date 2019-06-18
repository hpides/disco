package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

    // Slicing related
    private DistributedWindowMerger<Integer> windowMerger;

    public DistributedRoot(int controllerPort, int windowPort, String resultPath, int numChildren) {
        this.controllerPort = controllerPort;
        this.windowPort = windowPort;
        this.resultPath = resultPath;
        this.numChildren = numChildren;

        this.childStreamEnds = new HashSet<>(this.numChildren);

        this.context = new ZContext();
        this.windowPuller = this.context.createSocket(SocketType.PULL);
        this.windowPuller.bind(DistributedUtils.buildBindingTcpUrl(this.windowPort));
        this.resultPusher = this.context.createSocket(SocketType.PUSH);
        this.resultPusher.connect(DistributedUtils.buildIpcUrl(this.resultPath));
    }

    @Override
    public void run() {
        System.out.println(this.rootString("Starting root worker with controller port " + this.controllerPort +
                ", window port " + this.windowPort + " and result path " + this.resultPath));

        // 1. Wait for all children to register
        this.waitForChildren(this.numChildren);

        // 2. Process pre-aggregated windows
        this.waitForPreAggregatedWindows();
    }

    private void waitForPreAggregatedWindows() {
        while (!Thread.currentThread().isInterrupted()) {
            String childIdOrStreamEnd = this.windowPuller.recvStr();

            if (childIdOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                int childId = Integer.valueOf(this.windowPuller.recvStr(ZMQ.DONTWAIT));
                System.out.println(this.rootString("Stream end from CHILD-" + childId));
                this.childStreamEnds.add(childId);
                this.windowMerger.removeChild();
                if (this.childStreamEnds.size() == this.numChildren) {
                    System.out.println(this.rootString("Received all stream ends. Shutting down root..."));
                    this.resultPusher.send(DistributedUtils.STREAM_END);
                    return;
                }

                continue;
            }

            // System.out.println(this.rootString("Received from " + childId));
            String rawAggregateWindowId = this.windowPuller.recvStr(ZMQ.DONTWAIT);
            String rawPreAggregatedResult = this.windowPuller.recvStr(ZMQ.DONTWAIT);

            WindowAggregateId windowId = DistributedUtils.stringToWindowId(rawAggregateWindowId);
            Integer partialAggregate = Integer.valueOf(rawPreAggregatedResult);

            // TODO: fix!
            FunctionWindowAggregateId functionWindowAggId = new FunctionWindowAggregateId(windowId, 0);

            Optional<AggregateWindow> maybeFinalWindow = processPreAggregateWindow(functionWindowAggId, partialAggregate);
            if (maybeFinalWindow.isEmpty()) {
                continue;
            }

            AggregateWindow finalWindow = maybeFinalWindow.get();
            List aggValues = finalWindow.getAggValues();
            Object finalAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            byte[] finalAggregateBytes = DistributedUtils.objectToBytes(finalAggregate);

            // System.out.println(this.rootString("Sending result for " + windowId));
            this.resultPusher.sendMore(DistributedUtils.windowIdToString(windowId));
            this.resultPusher.send(finalAggregateBytes);
        }
    }

    public Optional<AggregateWindow> processPreAggregateWindow(FunctionWindowAggregateId windowId, Integer partialAggregate) {
        Optional<FunctionWindowAggregateId> triggerId = this.windowMerger.processPreAggregate(partialAggregate, windowId);
        if (triggerId.isPresent()) {
            AggregateWindow finalWindow = this.windowMerger.triggerFinalWindow(triggerId.get());
            return Optional.of(finalWindow);
        }

        return Optional.empty();
    }

    private void waitForChildren(int numChildren) {
        ZMQ.Socket childReceiver = this.context.createSocket(SocketType.REP);
        childReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.controllerPort));

//        String[] windowStrings = {"SLIDING,100,50,2"};
//        String[] windowStrings = {"TUMBLING,1000,1"};
//        String[] windowStrings = {"TUMBLING,10000,1", "SLIDING,10000,5000,2"};
//        String[] windowStrings = {"SESSION,1990,1"};
        String[] windowStrings = {"SESSION,1990,1", "TUMBLING,1000,2"};
        final long WATERMARK_MS = 1000;

        String[] aggFnStrings = {"SUM"};

        List<Window> windows = Arrays.stream(windowStrings).map(DistributedUtils::buildWindowFromString).collect(Collectors.toList());
        List<AggregateFunction> aggFn = Arrays.stream(aggFnStrings).map(DistributedUtils::buildAggregateFunctionFromString).collect(Collectors.toList());

        // Set up root the same way as the children will be set up.
        setupWindowMerger(windows, aggFn);

        String completeWindowString = String.join("\n", windowStrings);
        String completeAggFnString = String.join("\n", aggFnStrings);
        int numChildrenRegistered = 0;
        while (numChildrenRegistered < numChildren) {
                String message = childReceiver.recvStr();
                System.out.println(this.rootString("Received from child: " + message));

                childReceiver.sendMore(String.valueOf(WATERMARK_MS));
                childReceiver.sendMore(completeWindowString);
                childReceiver.send(completeAggFnString);
                numChildrenRegistered++;
        }
    }

    public void setupWindowMerger(List<Window> windows, List<AggregateFunction> aggFns) {
        this.windowMerger = new DistributedWindowMerger<>(this.numChildren, windows, aggFns);
    }

    private String rootString(String msg) {
        return "[ROOT] " + msg;
    }
}
