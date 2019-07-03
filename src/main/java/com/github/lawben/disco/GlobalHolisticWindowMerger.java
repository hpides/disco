package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.FunctionWindowId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import com.github.lawben.disco.aggregation.HolisticAggregateWrapper;
import com.github.lawben.disco.aggregation.HolisticNoopFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.state.DistributedAggregateWindowState;
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
    private final Map<Integer, List<DistributedSlice>> childSlices;
    private final Map<FunctionWindowId, FunctionWindowAggregateId> currentSessionWindowIds;
    private final List<AggregateFunction> aggFns;

    public GlobalHolisticWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        super(numChildren);
        this.aggFns = aggFunctions;
        this.stateFactory = new MemoryStateFactory();
        this.childSlices = new HashMap<>();
        this.currentSessionWindowIds = this.prepareSessionWindows(windows, aggFunctions);
    }

    @Override
    public Optional<FunctionWindowAggregateId> processPreAggregate(List<DistributedSlice> preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        final long windowId = functionWindowAggId.getWindowId().getWindowId();
        final int functionId = functionWindowAggId.getFunctionId();
        final FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);

        // Process session windows
        if (currentSessionWindowIds.containsKey(functionWindowId)) {
            throw new RuntimeException("holistic session not supported");
        }

        final int childId = functionWindowAggId.getChildId();
        childSlices.putIfAbsent(childId, new ArrayList<>());
        List<DistributedSlice> childSlices = this.childSlices.get(childId);
        childSlices.addAll(preAggregate);

        return checkWindowTrigger(functionWindowAggId);
    }

    @Override
    public AggregateWindow<List<DistributedSlice>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
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

        List<AggregateFunction> dummyFn = Collections.singletonList(this.aggFns.get(functionWindowId.getFunctionId()));
        AggregateState<List<DistributedSlice>> windowAgg = new AggregateState<>(this.stateFactory, dummyFn);
        windowAgg.addElement(finalSlices);
        return new DistributedAggregateWindowState<>(functionWindowId.getWindowId(), windowAgg);
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

        HolisticNoopFunction holisticMergeFunction = (HolisticNoopFunction) finalWindow.getAggregateFunctions().get(0);
        HolisticAggregateFunction originalFn = holisticMergeFunction.getOriginalFn();
        return (Integer) originalFn.lower(allValues);
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return new ArrayList<>(Collections.singletonList(new HolisticAggregateWrapper()));
    }
}
