package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
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

    public void processCountEvent(int eventValue, long eventTimestamp) {
        this.countBasedSlicer.processElement(eventValue, eventTimestamp);
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

    public Optional<WindowResult> processPreAggregateWindow(FunctionWindowAggregateId functionWindowId,
            String aggregateType, String rawPreAggregate, boolean windowIsComplete) {
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
            return Optional.empty();
        }

        FunctionWindowAggregateId finalWindowId = triggerId.get();
        AggregateWindow finalWindow = currentMerger.triggerFinalWindow(finalWindowId);
        Integer finalValue = currentMerger.lowerFinalValue(finalWindow);
        return Optional.of(new WindowResult(finalWindowId, finalValue));
    }
}
