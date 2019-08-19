package com.github.lawben.disco.merge;

import com.github.lawben.disco.DistributedChildSlicer;
import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.Event;
import com.github.lawben.disco.WindowResult;
import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.functions.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.BaseWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.state.AggregateWindowState;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AggregateMerger {
    private final DistributiveWindowMerger<Integer> distributiveWindowMerger;
    private final AlgebraicWindowMerger<AlgebraicPartial> algebraicWindowMerger;
    private final GlobalHolisticWindowMerger holisticWindowMerger;
    private final DistributedChildSlicer<Integer> countBasedSlicer;

    private final boolean hasDistributiveAggFns;
    private final boolean hasAlgebraicAggFns;
    private final boolean hasHolisticAggFns;

    private final List<WindowMerger> activeMergers;

    private WindowMerger currentMerger;

    public AggregateMerger(List<Window> windows, List<AggregateFunction> aggFns, int numChildren) {
        List<AggregateFunction> stateAggFunctions = DistributedUtils.convertAggregateFunctions(aggFns);

        hasDistributiveAggFns = aggFns.stream().anyMatch(aggFn -> aggFn instanceof DistributiveAggregateFunction);
        hasAlgebraicAggFns = aggFns.stream().anyMatch(aggFn -> aggFn instanceof AlgebraicAggregateFunction);
        hasHolisticAggFns = aggFns.stream().anyMatch(aggFn -> aggFn instanceof HolisticAggregateFunction);

        activeMergers = new ArrayList<>();
        distributiveWindowMerger = new DistributiveWindowMerger<>(numChildren, windows, stateAggFunctions);
        algebraicWindowMerger = new AlgebraicWindowMerger<>(numChildren, windows, stateAggFunctions);
        holisticWindowMerger = new GlobalHolisticWindowMerger(numChildren, windows, stateAggFunctions);
        if (hasDistributiveAggFns) {
            activeMergers.add(distributiveWindowMerger);
        }
        if (hasAlgebraicAggFns) {
            activeMergers.add(algebraicWindowMerger);
        }
        if (hasHolisticAggFns) {
            activeMergers.add(holisticWindowMerger);
        }

        List<Window> countWindows = windows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Count)
                .collect(Collectors.toList());

        this.countBasedSlicer = new DistributedChildSlicer<>(countWindows, aggFns);
    }

    public void initializeSessionStates(List<Integer> childIds) {
        activeMergers.forEach(merger -> merger.initializeSessionState(childIds));
    }

    public void processCountEvent(int eventValue, long eventTimestamp) {
        this.countBasedSlicer.processElement(eventValue, eventTimestamp);
    }

    public void processCountEvent(Event event) {
        processCountEvent(event.getValue(), event.getTimestamp());
    }

    public List<WindowResult> processCountWatermark(long watermarkTimestamp) {
        List<WindowResult> windowResults = new ArrayList<>();
        List<AggregateWindow> countWindows = this.countBasedSlicer.processWatermark(watermarkTimestamp);

        for (AggregateWindow countWindow : countWindows) {
            windowResults.addAll(this.processCountWindow((AggregateWindowState) countWindow));
        }

        return windowResults;
    }

    private List<WindowResult> processCountWindow(AggregateWindowState aggWindow) {
        List<WindowResult> windowResults = new ArrayList<>();
        WindowAggregateId windowId = aggWindow.getWindowAggregateId();

        final List aggValues = aggWindow.getAggValues();
        for (int functionId = 0; functionId < aggValues.size(); functionId++) {
            final Integer finalValue = (Integer) aggValues.get(functionId);
            FunctionWindowAggregateId functionWindowId = new FunctionWindowAggregateId(windowId, functionId);
            windowResults.add(new WindowResult(functionWindowId, finalValue));
        }

        return windowResults;
    }

    public List<DistributedAggregateWindowState> processWindowAggregates(FunctionWindowAggregateId functionWindowId, List<String> rawAggregates) {
        assert !rawAggregates.isEmpty();

        for (String rawWindowAggregate : rawAggregates) {
            String[] rawWindowAggregateParts = rawWindowAggregate.split(BaseWindowAggregate.DELIMITER);
            if (rawWindowAggregateParts.length != 3) {
                throw new IllegalArgumentException("Raw aggregate must consist of 3 parts, got: " + rawWindowAggregate);
            }

            String aggregateType = rawWindowAggregateParts[0];
            String rawAggregate = rawWindowAggregateParts[1];
            int key = Integer.parseInt(rawWindowAggregateParts[2]);

            int childId = functionWindowId.getChildId();
            FunctionWindowAggregateId keyedFunctionWindowId = new FunctionWindowAggregateId(functionWindowId, childId, key);
            currentMerger = this.processPreAggregateWindow(keyedFunctionWindowId, aggregateType, rawAggregate);
        }

        // Handle window complete
        Optional<FunctionWindowAggregateId> triggerId = currentMerger.checkWindowComplete(functionWindowId);
        if (triggerId.isEmpty()) {
            return new ArrayList<>();
        }

        return currentMerger.triggerFinalWindow(triggerId.get());
    }

    public WindowResult convertAggregateToWindowResult(DistributedAggregateWindowState aggState) {
        Integer finalValue = currentMerger.lowerFinalValue(aggState);
        return new WindowResult(aggState.getFunctionWindowId(), finalValue);
    }

    private WindowMerger processPreAggregateWindow(FunctionWindowAggregateId functionWindowId, String aggregateType, String rawPreAggregate) {
        switch (aggregateType) {
            case DistributedUtils.DISTRIBUTIVE_STRING:
                assert hasDistributiveAggFns;
                Integer partialAggregate = Integer.valueOf(rawPreAggregate);
                this.distributiveWindowMerger.processPreAggregate(partialAggregate, functionWindowId);
                return this.distributiveWindowMerger;
            case DistributedUtils.ALGEBRAIC_STRING:
                assert hasAlgebraicAggFns;
                List<AggregateFunction> algebraicFns = this.algebraicWindowMerger.getAggregateFunctions();
                AlgebraicMergeFunction algebraicMergeFn = (AlgebraicMergeFunction) algebraicFns.get(functionWindowId.getFunctionId());
                AlgebraicAggregateFunction algebraicFn = algebraicMergeFn.getOriginalFn();
                AlgebraicPartial partial = algebraicFn.partialFromString(rawPreAggregate);
                this.algebraicWindowMerger.processPreAggregate(partial, functionWindowId);
                return this.algebraicWindowMerger;
            case DistributedUtils.HOLISTIC_STRING:
                assert hasHolisticAggFns;
                List<DistributedSlice> slices = DistributedUtils.slicesFromString(rawPreAggregate);
                this.holisticWindowMerger.processPreAggregate(slices, functionWindowId);
                return this.holisticWindowMerger;
            default:
                throw new IllegalArgumentException("Unknown aggregate type: " + aggregateType);
        }
    }

    public FinalWindowsAndSessionStarts registerSessionStart(FunctionWindowAggregateId sessionStartId) {
        List<DistributedAggregateWindowState> finalWindows = new ArrayList<>();
        List<FunctionWindowAggregateId> newSessionStarts = new ArrayList<>();

        for (WindowMerger<?> windowMerger : activeMergers) {
            Optional<FunctionWindowAggregateId> newSessionId = windowMerger.registerSessionStart(sessionStartId);
            newSessionId.ifPresent(newSessionStarts::add);
            Optional<FunctionWindowAggregateId> triggerId = windowMerger.checkWindowComplete(sessionStartId);
            triggerId.ifPresent(functionWindowAggregateId -> finalWindows
                    .addAll(windowMerger.triggerFinalWindow(functionWindowAggregateId)));
        }

        List<FunctionWindowAggregateId> uniqueSessionStarts =
                new ArrayList<>(newSessionStarts.stream().collect(Collectors.groupingBy(fId -> fId)).keySet());

        return new FinalWindowsAndSessionStarts(finalWindows, uniqueSessionStarts);
    }

    public List<FunctionWindowAggregateId> getSessionStarts(FunctionWindowAggregateId lastSession) {
        List<FunctionWindowAggregateId> sessionStarts = new ArrayList<>();
        for (WindowMerger<?> merger : activeMergers) {
            merger.getSessionStart(lastSession).ifPresent(sessionStarts::add);
        }

        // Get unique session starts, to avoid duplicate sending and computing
        return new ArrayList<>(sessionStarts.stream().collect(Collectors.groupingBy(fId -> fId)).keySet());
    }
}
