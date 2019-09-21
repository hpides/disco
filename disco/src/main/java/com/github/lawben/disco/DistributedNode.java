package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.CONTROL_STRING;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.EVENT_STRING;
import static com.github.lawben.disco.DistributedUtils.HOLISTIC_STRING;
import static com.github.lawben.disco.DistributedUtils.createAggFunctionsFromString;
import static com.github.lawben.disco.DistributedUtils.createWindowsFromString;
import static com.github.lawben.disco.Event.NO_KEY;
import static com.github.lawben.disco.aggregation.FunctionWindowAggregateId.NO_CHILD_ID;

import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.AlgebraicWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.DistributiveWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.merge.AggregateMerger;
import com.github.lawben.disco.merge.FinalWindowsAndSessionStarts;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class DistributedNode {
    private boolean interrupt;
    private final String nodeIdentifier;
    protected final int nodeId;

    protected final ZContext context;
    protected ZMQ.Socket controlListener;
    protected ZMQ.Socket controlSender;
    protected ZMQ.Socket dataPuller;
    protected ZMQ.Socket windowPusher;

    protected String[] windowStrings;
    protected String[] aggregateFnStrings;

    // Parent part
    final int controllerPort;
    final int dataPort;
    final int numChildren;
    final Set<Integer> childEnds;
    AggregateMerger aggregateMerger;

    // Child part
    final String parentIp;
    final int parentControllerPort;
    final int parentWindowPort;
    protected long watermarkMs;

    public DistributedNode(int nodeId, String nodeIdentifier, int controllerPort, int dataPort, int numChildren,
            String parentIp, int parentControllerPort, int parentWindowPort) {
        this.nodeId = nodeId;
        this.nodeIdentifier = nodeIdentifier;
        this.context = new ZContext();
        this.interrupt = false;
        this.controllerPort = controllerPort;
        this.dataPort = dataPort;
        this.numChildren = numChildren;
        this.childEnds = new HashSet<>(this.numChildren);
        this.parentIp = parentIp;
        this.parentControllerPort = parentControllerPort;
        this.parentWindowPort = parentWindowPort;
    }

    public void processAndSendWindowAggregates() {
        FinalWindowsAndSessionStarts finalWindowsAndSessionStarts = processWindowAggregates();
        sendPreAggregatedWindowsToParent(finalWindowsAndSessionStarts.getFinalWindows());
        sendSessionStartsToParent(finalWindowsAndSessionStarts.getNewSessionStarts());
    }

    public FinalWindowsAndSessionStarts processWindowAggregates() {
        String rawFunctionWindowAggId = this.dataPuller.recvStr(ZMQ.DONTWAIT);
        int numAggregates = Integer.parseInt(dataPuller.recvStr(ZMQ.DONTWAIT));

        List<String> rawPreAggregates = new ArrayList<>(numAggregates);
        for (int i = 0; i < numAggregates; i++) {
            rawPreAggregates.add(dataPuller.recvStr(ZMQ.DONTWAIT));
        }

        FunctionWindowAggregateId functionWindowAggId =
                DistributedUtils.stringToFunctionWindowAggId(rawFunctionWindowAggId);

//        System.out.println(nodeString("Processing: " + functionWindowAggId + " with " + rawPreAggregates));
        List<DistributedAggregateWindowState> finalWindows =
                this.aggregateMerger.processWindowAggregates(functionWindowAggId, rawPreAggregates);
        List<FunctionWindowAggregateId> sessionStarts = aggregateMerger.getSessionStarts(functionWindowAggId);
        return new FinalWindowsAndSessionStarts(finalWindows, sessionStarts);
    }

    public FinalWindowsAndSessionStarts handleControlInput() {
        String controlMsg = dataPuller.recvStr(ZMQ.DONTWAIT);
        FunctionWindowAggregateId sessionStartId = DistributedUtils.stringToFunctionWindowAggId(controlMsg);
        System.out.println(nodeString("Registering session start " + sessionStartId));
        return aggregateMerger.registerSessionStart(sessionStartId);
    }

    public void sendSessionStartsToParent(List<FunctionWindowAggregateId> sessionStarts) {
        for (FunctionWindowAggregateId sessionStart : sessionStarts) {
            sendSessionStartToParent(sessionStart);
        }
    }

    public void sendSessionStartToParent(FunctionWindowAggregateId sessionStart) {
        WindowAggregateId windowAggregateId = sessionStart.getWindowId();
        FunctionWindowAggregateId fullSessionStartId =
                new FunctionWindowAggregateId(windowAggregateId, 0, nodeId, sessionStart.getKey());
        String sessionMsg = DistributedUtils.functionWindowIdToString(fullSessionStartId);
        sendToParent(Arrays.asList(CONTROL_STRING, sessionMsg));
    }

    public void sendPreAggregatedWindowsToParent(List<DistributedAggregateWindowState> preAggregatedWindows) {
        Map<FunctionWindowAggregateId, List<DistributedAggregateWindowState>> keyedAggWindows =
                preAggregatedWindows
                        .stream()
                        .collect(Collectors.groupingBy(windowState ->
                                // Remove child id and key because we want to collect those per window.
                                new FunctionWindowAggregateId(windowState.getFunctionWindowId(), NO_CHILD_ID, NO_KEY))
                        );

        List<List<DistributedAggregateWindowState>> sortedAggWindows = keyedAggWindows.values().stream()
                .sorted(Comparator.comparingInt(y -> y.get(0).getFunctionWindowId().getFunctionId()))
                .sorted(Comparator.comparingLong(y -> y.get(0).getStart()))
                .collect(Collectors.toList());

        for (List<DistributedAggregateWindowState> aggWindowsPerKey : sortedAggWindows) {
            FunctionWindowAggregateId functionWindowAggId = aggWindowsPerKey.get(0).getFunctionWindowId();
            FunctionWindowAggregateId withChildId = functionWindowAggId.withChildId(this.nodeId);
            List<String> serializedMessage =
                    this.serializedFunctionWindows(withChildId, aggWindowsPerKey);
            this.sendToParent(serializedMessage);
        }
    }

    private List<String> serializedFunctionWindows(FunctionWindowAggregateId functionWindowAggId,
            List<DistributedAggregateWindowState> aggWindows) {
        List<String> serializedMessage = new ArrayList<>();
        // Add child id and window id for each aggregate type
        serializedMessage.add(String.valueOf(this.nodeId));
        serializedMessage.add(DistributedUtils.functionWindowIdToString(functionWindowAggId));
        serializedMessage.add(String.valueOf(aggWindows.size()));

        for (DistributedAggregateWindowState preAggregatedWindow : aggWindows) {
            String serializedAggWindow = this.serializeAggregate(preAggregatedWindow);
            serializedMessage.add(serializedAggWindow);
        }

        return serializedMessage;
    }

    private String serializeAggregate(DistributedAggregateWindowState aggWindow) {
        List<AggregateFunction> aggFns = aggWindow.getAggregateFunctions();
        if (aggFns.size() > 1) {
            throw new IllegalStateException("Final agg should only have one function.");
        }
        FunctionWindowAggregateId functionWindowAggId = aggWindow.getFunctionWindowId();
        final int key = functionWindowAggId.getKey();

        List aggValues = aggWindow.getAggValues();
        boolean hasValue = !aggValues.isEmpty();
        AggregateFunction aggFn = aggFns.get(0);

        if (aggFn instanceof DistributiveAggregateFunction) {
            Long partialAggregate = hasValue ? (Long) aggValues.get(0) : null;
            return new DistributiveWindowAggregate(partialAggregate, key).asString();
        } else if (aggFn instanceof AlgebraicMergeFunction) {
            AlgebraicPartial partial = hasValue ? (AlgebraicPartial) aggValues.get(0) : null;
            return new AlgebraicWindowAggregate(partial, key).asString();
        } else if (aggFn instanceof HolisticMergeWrapper) {
            List<Slice> slices = hasValue ? (List<Slice>) aggValues.get(0) : new ArrayList<>();
            // Add functionId as slice might have multiple states
            String slicesString = DistributedUtils.slicesToString(slices, functionWindowAggId.getFunctionId());
            return HOLISTIC_STRING + ":" + slicesString + ":" + key;
        } else {
            throw new IllegalArgumentException("Unknown aggregate function type: " + aggFn.getClass().getSimpleName());
        }
    }

    public void sendToParent(List<String> serializedMessage) {
//        System.out.println(nodeString("Sending to parent: " + serializedMessage));
        for (int i = 0; i < serializedMessage.size() - 1; i++) {
            this.windowPusher.sendMore(serializedMessage.get(i));
        }
        this.windowPusher.send(serializedMessage.get(serializedMessage.size() - 1));
    }

    public boolean isTotalStreamEnd() {
        int childId = Integer.parseInt(dataPuller.recvStr(ZMQ.DONTWAIT));
        System.out.println(this.nodeString("Stream end from child " + childId));
        this.childEnds.add(childId);
        return this.childEnds.size() >= this.numChildren;
    }

    public void waitForChildren() {
        ZMQ.Socket childReceiver = createRegistrationListener();

        List<Window> windows = Arrays.stream(this.windowStrings)
                .map(DistributedUtils::buildWindowFromString)
                .collect(Collectors.toList());

        final long watermarkMs = DistributedUtils.getWatermarkMsFromWindowString(this.windowStrings);

        List<AggregateFunction> aggFn = Arrays.stream(this.aggregateFnStrings)
                .map(DistributedUtils::buildAggregateFunctionFromString)
                .collect(Collectors.toList());

        // Set up root the same way as the children will be set up.
        this.aggregateMerger = new AggregateMerger(windows, aggFn, this.numChildren);
        List<Integer> childIds = new ArrayList<>(this.numChildren);

        String completeWindowString = String.join("\n", this.windowStrings);
        String completeAggFnString = String.join("\n", this.aggregateFnStrings);
        int numChildrenRegistered = 0;
        while (!isInterrupted() && numChildrenRegistered < numChildren) {
            String message = childReceiver.recvStr();
            if (message == null) {
                continue;
            }
            System.out.println(this.nodeString("Received from child: " + message));
            Pattern childIdPattern = Pattern.compile("(CHILD|NODE|MERGER)-(\\d+).*");
            Matcher matcher = childIdPattern.matcher(message);
            if (matcher.find()) {
                Integer childId = Integer.valueOf(matcher.group(2));
                childIds.add(childId);
            } else {
                throw new IllegalArgumentException("Cannot get child id from " + message);
            }

            childReceiver.sendMore(String.valueOf(watermarkMs));
            childReceiver.sendMore(completeWindowString);
            childReceiver.send(completeAggFnString);
            numChildrenRegistered++;
        }

        if (isInterrupted()) {
            return;
        }

        recreateControlListener();
        this.aggregateMerger.initializeSessionStates(childIds);
    }

    public WindowingConfig registerAtParent() {
        createRegistrationSender();
        controlSender.send(this.nodeString("I am a new node."));

        this.watermarkMs = Long.parseLong(controlSender.recvStr());
        String windowString = controlSender.recvStr();
        String aggString = controlSender.recvStr();

        this.windowStrings = windowString.split(";");
        this.aggregateFnStrings = aggString.split(";");
        System.out.println(this.nodeString("Received: " + this.watermarkMs +
                " ms watermark | " + Arrays.toString(windowStrings) + " | " + Arrays.toString(aggregateFnStrings)));

        this.windowPusher = this.context.createSocket(SocketType.PUSH);
        this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.parentIp, this.parentWindowPort));

        List<Window> allWindows = createWindowsFromString(windowString);
        List<AggregateFunction> aggregateFunctions = createAggFunctionsFromString(aggString);

        if (aggregateFunctions.isEmpty()) {
            throw new RuntimeException(this.nodeString("Did not receive any aggFns from root!"));
        }

        List<Window> timeWindows = allWindows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Time)
                .collect(Collectors.toList());

        List<Window> countWindows = allWindows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Count)
                .collect(Collectors.toList());

        if (timeWindows.isEmpty() && countWindows.isEmpty()) {
            throw new RuntimeException(this.nodeString("Did not receive any windows from root!"));
        }

        return new WindowingConfig(timeWindows, countWindows, aggregateFunctions);
    }

    public void forwardEvent(String eventString) {
        sendToParent(Arrays.asList(EVENT_STRING, eventString));
    }

    public ZMQ.Socket createRegistrationSender() {
        if (this.controlSender == null) {
            this.controlSender = this.context.createSocket(SocketType.REQ);
            this.controlSender.connect(DistributedUtils.buildTcpUrl(this.parentIp, this.parentControllerPort));
        }
        return this.controlSender;
    }

    public ZMQ.Socket createDataPuller() {
        return this.createDataPuller(this.dataPort);
    }

    public ZMQ.Socket createDataPuller(int port) {
        if (this.dataPuller == null) {
            this.dataPuller = this.context.createSocket(SocketType.PULL);
            this.dataPuller.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
            this.dataPuller.setRcvHWM(1000);

            int retries = 0;
            while (true) {
                try {
                    this.dataPuller.bind(DistributedUtils.buildBindingTcpUrl(port));
                    break;
                } catch (ZMQException e) {
                    if (++retries == 50) {
                        throw new RuntimeException("Still fails after 100 retries. ", e);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {}
                }

            }
        }
        return this.dataPuller;
    }

    public ZMQ.Socket createWindowPusher(String ip, int port) {
        if (this.windowPusher == null){
            this.windowPusher = this.context.createSocket(SocketType.PUSH);
            this.windowPusher.connect(DistributedUtils.buildTcpUrl(ip, port));
        }
        return this.windowPusher;
    }

    public ZMQ.Socket createControlListener() {
        if (this.controlListener == null) {
            this.controlListener = this.context.createSocket(SocketType.PULL);
            this.controlListener.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
            while (true) {
                try {
                    this.controlListener.bind(DistributedUtils.buildBindingTcpUrl(controllerPort));
                    break;
                } catch (ZMQException e) {}
            }
        }
        return this.controlListener;
    }

    public ZMQ.Socket createRegistrationListener() {
        if (this.controlListener == null) {
            this.controlListener = this.context.createSocket(SocketType.REP);
            this.controlListener.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
            this.controlListener.bind(DistributedUtils.buildBindingTcpUrl(controllerPort));
        }
        return this.controlListener;
    }

    public ZMQ.Socket recreateControlListener() {
        controlListener.setLinger(0);
        controlListener.close();
        controlListener.setLinger(0);
        controlListener = null;
        return createControlListener();
    }

    void interrupt() {
        this.interrupt = true;
    }

    boolean isInterrupted() {
        return this.interrupt;
    }

    public String nodeString(String msg) {
        return "[" + this.nodeIdentifier + "-" + this.nodeId + "] " + msg;
    }

    public void endChild() {
        this.windowPusher.sendMore(DistributedUtils.STREAM_END);
        this.windowPusher.send(String.valueOf(this.nodeId));
    }

    public void close() {
        closeIfNotNull(windowPusher);
        closeIfNotNull(dataPuller);
        closeIfNotNull(context);
    }

    private void closeIfNotNull(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {}
        }
    }
}
