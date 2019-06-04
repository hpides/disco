package com.github.lawben.disco;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
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
    Set<Integer> childStreamEnds;

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
            byte[] rawPreAggregatedResult = this.windowPuller.recv(ZMQ.DONTWAIT);

            WindowAggregateId windowId = DistributedUtils.stringToWindowId(rawAggregateWindowId);

            // Partial Aggregate
            Object partialAggregateObject = DistributedUtils.bytesToObject(rawPreAggregatedResult);
            Integer partialAggregate = (Integer) partialAggregateObject;

            Optional<AggregateWindow> maybeFinalWindow = processPreAggregateWindow(windowId, partialAggregate);
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

    public Optional<AggregateWindow> processPreAggregateWindow(WindowAggregateId windowId, Integer partialAggregate) {
        boolean triggerFinal = this.windowMerger.processPreAggregate(partialAggregate, windowId);
        if (triggerFinal) {
            AggregateWindow finalWindow = this.windowMerger.triggerFinalWindow(windowId);
            return Optional.of(finalWindow);
        }

        return Optional.empty();
    }

    private void waitForChildren(int numChildren) {
        ZMQ.Socket childReceiver = this.context.createSocket(SocketType.REP);
        childReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.controllerPort));

//        String[] windowStrings = {"SLIDING,100,50,2"};
        String[] windowStrings = {"TUMBLING,1000,1"};
//        String[] windowStrings = {"TUMBLING,10000,1", "SLIDING,10000,5000,2"};
        final long WATERMARK_MS = 1000;

        final ReduceAggregateFunction<Integer> aggFn = DistributedUtils.aggregateFunctionSum();
        List<Window> windows = Arrays.stream(windowStrings).map(DistributedUtils::buildWindowFromString).collect(Collectors.toList());

        // Set up root the same way as the children will be set up.
        setupWindowMerger(windows, aggFn);

        String completeWindowString = String.join("\n", windowStrings);
        int numChildrenRegistered = 0;
        while (numChildrenRegistered < numChildren) {
                String message = childReceiver.recvStr();
                System.out.println(this.rootString("Received from child: " + message));

                childReceiver.sendMore(String.valueOf(WATERMARK_MS));
                childReceiver.send(completeWindowString);
                numChildrenRegistered++;
        }
    }

    public void setupWindowMerger(List<Window> windows, ReduceAggregateFunction<Integer> aggFn) {
        this.windowMerger = new DistributedWindowMerger<>(new MemoryStateFactory(), this.numChildren, windows, aggFn);
    }

    private String rootString(String msg) {
        return "[ROOT] " + msg;
    }
}
