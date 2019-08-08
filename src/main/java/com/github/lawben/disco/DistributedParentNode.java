package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.HOLISTIC_STRING;
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
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

class DistributedParentNode extends DistributedNode {
    final int controllerPort;
    final int dataPort;
    final int numChildren;
    final Set<Integer> childStreamEnds;
    AggregateMerger aggregateMerger;
    String[] windowStrings;
    String[] aggregateFnStrings;

    public DistributedParentNode(int nodeId, String nodeIdentifier, int controllerPort, int dataPort, int numChildren) {
        super(nodeId, nodeIdentifier);
        this.controllerPort = controllerPort;
        this.dataPort = dataPort;
        this.numChildren = numChildren;
        this.childStreamEnds = new HashSet<>(this.numChildren);
    }

    public void processAndSendWindowAggregates() {
        sendPreAggregatedWindowsToParent(processWindowAggregates());
    }

    public List<DistributedAggregateWindowState> processWindowAggregates() {
        String rawFunctionWindowAggId = this.dataPuller.recvStr(ZMQ.DONTWAIT);
        int numAggregates = Integer.parseInt(dataPuller.recvStr(ZMQ.DONTWAIT));

        List<String> rawPreAggregates = new ArrayList<>(numAggregates);
        for (int i = 0; i < numAggregates; i++) {
            rawPreAggregates.add(dataPuller.recvStr(ZMQ.DONTWAIT));
        }

        FunctionWindowAggregateId functionWindowAggId =
                DistributedUtils.stringToFunctionWindowAggId(rawFunctionWindowAggId);

        return this.aggregateMerger.processWindowAggregates(functionWindowAggId, rawPreAggregates);
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

        for (var aggWindowsPerKey : sortedAggWindows) {
            FunctionWindowAggregateId functionWindowAggId = aggWindowsPerKey.get(0).getFunctionWindowId();
            List<String> serializedMessage =
                    this.serializedFunctionWindows(functionWindowAggId, aggWindowsPerKey);
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
            Integer partialAggregate = hasValue ? (Integer) aggValues.get(0) : null;
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
        for (int i = 0; i < serializedMessage.size() - 1; i++) {
            this.windowPusher.sendMore(serializedMessage.get(i));
        }
        this.windowPusher.send(serializedMessage.get(serializedMessage.size() - 1), ZMQ.DONTWAIT);
    }

    public boolean isTotalStreamEnd() {
        int childId = Integer.parseInt(dataPuller.recvStr(ZMQ.DONTWAIT));
        System.out.println(this.nodeString("Stream end from child " + childId));
        this.childStreamEnds.add(childId);
        return this.childStreamEnds.size() >= this.numChildren;
    }

    public void waitForChildren() {
        ZMQ.Socket childReceiver = this.context.createSocket(SocketType.REP);
        childReceiver.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        childReceiver.bind(DistributedUtils.buildBindingTcpUrl(this.controllerPort));

        List<Window> windows = Arrays.stream(this.windowStrings)
                .map(DistributedUtils::buildWindowFromString)
                .collect(Collectors.toList());

        List<Window> timedWindows = windows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Time)
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
            Pattern childIdPattern = Pattern.compile("(CHILD|NODE)-(\\d+).*");
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

        this.aggregateMerger.initializeSessionStates(childIds);
    }

    public ZMQ.Socket createDataPuller() {
        return this.createDataPuller(this.dataPort);
    }
}
