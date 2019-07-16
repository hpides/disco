package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.ChildKey;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

public class GlobalHolisticWindowMerger extends BaseWindowMerger<List<DistributedSlice>> {
    private final StateFactory stateFactory;
    private final Map<ChildKey, List<DistributedSlice>> childSlices;
    private final List<AggregateFunction> aggFns;

    public GlobalHolisticWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        super(numChildren, windows, aggFunctions);
        this.aggFns = aggFunctions;
        this.stateFactory = new MemoryStateFactory();
        this.childSlices = new HashMap<>();
    }

    public Optional<FunctionWindowAggregateId> processPreAggregateAndCheckComplete(List<DistributedSlice> preAggregate, FunctionWindowAggregateId functionWindowAggId, boolean windowIsComplete) {
        Optional<FunctionWindowAggregateId> triggerId = this.processPreAggregate(preAggregate, functionWindowAggId);

        if (triggerId.isPresent()) {
            return triggerId;
        }

        if (!this.isSessionWindow(functionWindowAggId) && windowIsComplete) {
            return this.checkWindowTrigger(functionWindowAggId);
        }

        return Optional.empty();
    }

    @Override
    public Optional<FunctionWindowAggregateId> processPreAggregate(List<DistributedSlice> preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        if (this.isSessionWindow(functionWindowAggId)) {
            return this.processSessionWindow(preAggregate, functionWindowAggId);
        }

        ChildKey childKey = ChildKey.fromFunctionWindowId(functionWindowAggId);
        childSlices.putIfAbsent(childKey, new ArrayList<>());
        List<DistributedSlice> childSlices = this.childSlices.get(childKey);
        childSlices.addAll(preAggregate);

        return Optional.empty();
    }

    @Override
    public DistributedAggregateWindowState<List<DistributedSlice>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        if (this.isSessionWindow(functionWindowId)) {
            AggregateState<List<DistributedSlice>> windowAgg = this.windowAggregates.remove(functionWindowId);
            return new DistributedAggregateWindowState<>(functionWindowId, windowAgg);
        }

        WindowAggregateId windowId = functionWindowId.getWindowId();
        final long windowStart = windowId.getWindowStartTimestamp();
        final long windowEnd = windowId.getWindowEndTimestamp();

        List<DistributedSlice> finalSlices = new ArrayList<>();
        for (List<DistributedSlice> slices : childSlices.values()) {
            ListIterator<DistributedSlice> iterator = slices.listIterator(slices.size());
            while (iterator.hasPrevious()) {
                DistributedSlice slice = iterator.previous();
                if (slice.getTStart() >= windowStart) {
                    if (slice.getTEnd() <= windowEnd) {
                        finalSlices.add(slice);
                    }
                } else {
                    break;
                }
            }
        }

        List<AggregateFunction> noOpFn = Collections.singletonList(this.aggFns.get(functionWindowId.getFunctionId()));
        AggregateState<List<DistributedSlice>> windowAgg = new AggregateState<>(this.stateFactory, noOpFn);
        windowAgg.addElement(finalSlices);
        return new DistributedAggregateWindowState<>(functionWindowId, windowAgg);
    }

    @Override
    public Integer lowerFinalValue(AggregateWindow finalWindow) {
        List aggValues = finalWindow.getAggValues();
        if (aggValues.isEmpty()) {
            throw new IllegalStateException("Cannot have empty slice list in holistic merge");
        }

        List<DistributedSlice> slices = (List<DistributedSlice>) aggValues.get(0);
        int totalSize = slices.stream().map(slice -> slice.getValues().size()).reduce(0, Integer::sum);

        List<Integer> allValues = new ArrayList<>(totalSize);
        for (DistributedSlice slice : slices) {
            allValues.addAll(slice.getValues());
        }

        HolisticMergeWrapper holisticMergeFunction = (HolisticMergeWrapper) finalWindow.getAggregateFunctions().get(0);
        HolisticAggregateFunction originalFn = holisticMergeFunction.getOriginalFn();
        return (Integer) originalFn.lower(allValues);
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return new ArrayList<>(Collections.singletonList(new HolisticMergeWrapper()));
    }
}
