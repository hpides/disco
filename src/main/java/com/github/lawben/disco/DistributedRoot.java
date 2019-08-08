package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.STREAM_END;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class DistributedRoot implements Runnable {
    private final static String NODE_IDENTIFIER = "ROOT";
    private final DistributedParentNode parentNodeImpl;

    private final String resultPath;
    private final ZMQ.Socket resultPusher;

    private long watermarkMs;

    // Count-related
    long currentEventTime = 0;
    long lastWatermark = 0;
    long numEvents = 0;

    public DistributedRoot(int controllerPort, int windowPort, String resultPath, int numChildren, String windowsString, String aggregateFunctionsString) {
        this.parentNodeImpl = new DistributedParentNode(0, NODE_IDENTIFIER, controllerPort, windowPort, numChildren);
        this.resultPath = resultPath;

        parentNodeImpl.windowStrings = windowsString.split(";");
        if (parentNodeImpl.windowStrings.length == 0) {
            throw new IllegalArgumentException("Need at least one window.");
        }

        parentNodeImpl.aggregateFnStrings = aggregateFunctionsString.split(";");
        if (parentNodeImpl.aggregateFnStrings.length == 0) {
            throw new IllegalArgumentException("Need at least one aggregate function.");
        }

        this.watermarkMs = DistributedUtils.getWatermarkMsFromWindowString(parentNodeImpl.windowStrings);

        parentNodeImpl.createDataPuller();
        this.resultPusher = parentNodeImpl.context.createSocket(SocketType.PUSH);
        this.resultPusher.connect(DistributedUtils.buildIpcUrl(this.resultPath));
    }

    @Override
    public void run() {
        System.out.println(this.rootString("Starting root worker with controller port " + parentNodeImpl.controllerPort +
                ", window port " + parentNodeImpl.dataPort + " and result path " + this.resultPath));

        try {
            parentNodeImpl.waitForChildren();
            this.processPreAggregatedWindows();
        } finally {
            parentNodeImpl.close();
        }
    }

    private void processPreAggregatedWindows() {
        ZMQ.Socket windowPuller = parentNodeImpl.dataPuller;

        while (!parentNodeImpl.isInterrupted()) {
            String messageOrStreamEnd = windowPuller.recvStr();
            if (messageOrStreamEnd == null) {
                continue;
            }

            final List<WindowResult> windowResults;
            if (messageOrStreamEnd.equals(STREAM_END)) {
                if (parentNodeImpl.isTotalStreamEnd()) {
                    System.out.println(this.rootString("Processed " + numEvents + " count-events in total."));
                    resultPusher.send(STREAM_END);
                    return;
                }
                continue;
            } else if (messageOrStreamEnd.equals(DistributedUtils.EVENT_STRING)) {
                windowResults = processCountEvent();
            } else {
                windowResults = parentNodeImpl.processWindowAggregates().stream()
                        .map(state -> parentNodeImpl.aggregateMerger.convertAggregateToWindowResult(state))
                        .collect(Collectors.toList());
            }

            windowResults.forEach(this::sendResult);
        }
    }

    private List<WindowResult> processCountEvent() {
        ZMQ.Socket windowPuller = parentNodeImpl.dataPuller;
        String rawEvent = windowPuller.recvStr(ZMQ.DONTWAIT);
        Event event = Event.fromString(rawEvent);
        parentNodeImpl.aggregateMerger.processCountEvent(event);

        currentEventTime = event.getTimestamp();
        numEvents++;
        final long maxLateness = this.watermarkMs;
        final long watermarkTimestamp = lastWatermark + this.watermarkMs;
        if (currentEventTime < watermarkTimestamp + maxLateness) {
            return new ArrayList<>();
        }

        lastWatermark = watermarkTimestamp;
        return parentNodeImpl.aggregateMerger.processCountWatermark(watermarkTimestamp);
    }

    private void sendResult(WindowResult windowResult) {
        String finalAggregateString = String.valueOf(windowResult.getValue());
        this.resultPusher.sendMore(DistributedUtils.functionWindowIdToString(windowResult.getFinalWindowId()));
        this.resultPusher.send(finalAggregateString, ZMQ.DONTWAIT);
    }

    private String rootString(String msg) {
        return "[ROOT] " + msg;
    }

    public void interrupt() {
        parentNodeImpl.interrupt();
    }
}
