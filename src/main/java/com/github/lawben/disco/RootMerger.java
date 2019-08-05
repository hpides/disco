package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.BaseWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
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

public class RootMerger {
    private DistributiveWindowMerger<Integer> distributiveWindowMerger;
    private AlgebraicWindowMerger<AlgebraicPartial> algebraicWindowMerger;
    private GlobalHolisticWindowMerger holisticWindowMerger;
    private DistributedChildSlicer<Integer> countBasedSlicer;

    public RootMerger(List< Window > windows, List<AggregateFunction> aggFns, int numChildren) {
        List<AggregateFunction> stateAggFunctions = DistributedUtils.convertAggregateFunctions(aggFns);

        this.distributiveWindowMerger = new DistributiveWindowMerger<>(numChildren, windows, stateAggFunctions);
        this.algebraicWindowMerger = new AlgebraicWindowMerger<>(numChildren, windows, stateAggFunctions);
        this.holisticWindowMerger = new GlobalHolisticWindowMerger(numChildren, windows, stateAggFunctions);

        List<Window> countWindows = windows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Count)
                .collect(Collectors.toList());

        this.countBasedSlicer = new DistributedChildSlicer<>(countWindows, aggFns);
    }

    public void initializeSessionStates(List<Integer> childIds) {
        this.distributiveWindowMerger.initializeSessionState(childIds);
        this.algebraicWindowMerger.initializeSessionState(childIds);
        this.holisticWindowMerger.initializeSessionState(childIds);
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

    public List<WindowResult> processWindowAggregates(FunctionWindowAggregateId functionWindowId, List<String> rawAggregates) {
        assert !rawAggregates.isEmpty();

        WindowMerger currentMerger = null;
        for (String rawWindowAggregate : rawAggregates) {
            String[] rawWindowAggregateParts = rawWindowAggregate.split(BaseWindowAggregate.DELIMITER);
            if (rawWindowAggregateParts.length != 3) {
                throw new IllegalArgumentException("Raw aggregate must consist of 3 parts, got: " + rawWindowAggregate);
            }

            String aggregateType = rawWindowAggregateParts[0];
            String rawAggregate = rawWindowAggregateParts[1];
            int key = Integer.valueOf(rawWindowAggregateParts[2]);

            int childId = functionWindowId.getChildId();
            FunctionWindowAggregateId keyedFunctionWindowId = new FunctionWindowAggregateId(functionWindowId, childId, key);
            currentMerger = this.processPreAggregateWindow(keyedFunctionWindowId, aggregateType, rawAggregate);
        }

        // Handle window complete
        Optional<FunctionWindowAggregateId> triggerId = currentMerger.checkWindowComplete(functionWindowId);
        if (triggerId.isEmpty()) {
            return new ArrayList<>();
        }

        List<WindowResult> windowResults = new ArrayList<>();
        List<DistributedAggregateWindowState> finalWindows = currentMerger.triggerFinalWindow(triggerId.get());
        for (DistributedAggregateWindowState finalWindow : finalWindows) {
            Integer finalValue = currentMerger.lowerFinalValue(finalWindow);
            windowResults.add(new WindowResult(finalWindow.getFunctionWindowId(), finalValue));
        }

        return windowResults;
    }

    public WindowMerger processPreAggregateWindow(FunctionWindowAggregateId functionWindowId, String aggregateType, String rawPreAggregate) {
        switch (aggregateType) {
            case DistributedUtils.DISTRIBUTIVE_STRING:
                Integer partialAggregate = Integer.valueOf(rawPreAggregate);
                this.distributiveWindowMerger.processPreAggregate(partialAggregate, functionWindowId);
                return this.distributiveWindowMerger;
            case DistributedUtils.ALGEBRAIC_STRING:
                List<AggregateFunction> algebraicFns = this.algebraicWindowMerger.getAggregateFunctions();
                AlgebraicMergeFunction algebraicMergeFn = (AlgebraicMergeFunction) algebraicFns.get(functionWindowId.getFunctionId());
                AlgebraicAggregateFunction algebraicFn = algebraicMergeFn.getOriginalFn();
                AlgebraicPartial partial = algebraicFn.partialFromString(rawPreAggregate);
                this.algebraicWindowMerger.processPreAggregate(partial, functionWindowId);
                return this.algebraicWindowMerger;
            case DistributedUtils.HOLISTIC_STRING:
                List<DistributedSlice> slices = DistributedUtils.slicesFromString(rawPreAggregate);
                this.holisticWindowMerger.processPreAggregate(slices, functionWindowId);
                return this.holisticWindowMerger;
            default:
                throw new IllegalArgumentException("Unknown aggregate type: " + aggregateType);
        }
    }
}
