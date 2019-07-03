package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticNoopFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.state.DistributedAggregateWindowState;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LocalHolisticWindowMerger implements WindowMerger<List<Slice>> {
    private final StateFactory stateFactory;
    private final Map<Integer, Set<Long>> seenSlices;
    private final Map<FunctionWindowAggregateId, AggregateState<List<Slice>>> aggregates;

    public LocalHolisticWindowMerger() {
        this.stateFactory = new MemoryStateFactory();
        this.seenSlices = new HashMap<>();
        this.aggregates = new HashMap<>();
    }

    @Override
    public Optional<FunctionWindowAggregateId> processPreAggregate(List<Slice> preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        final int childId = functionWindowAggId.getChildId();
        seenSlices.putIfAbsent(childId, new HashSet<>());

        Set<Long> seenChildSlices = seenSlices.get(childId);
        List<Slice> newSlices = new ArrayList<>(preAggregate.size());

        for (Slice slice : preAggregate) {
            if (seenChildSlices.contains(slice.getTStart())) {
                continue;
            }

            newSlices.add(slice);
            seenChildSlices.add(slice.getTStart());
        }

        List<AggregateFunction> dummyFn = Collections.singletonList(new HolisticNoopFunction());
        AggregateState<List<Slice>> windowAgg = new AggregateState<>(this.stateFactory, dummyFn);
        windowAgg.addElement(newSlices);

        aggregates.put(functionWindowAggId, windowAgg);
        return Optional.of(functionWindowAggId);
    }

    @Override
    public AggregateWindow<List<Slice>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        AggregateWindow<List<Slice>> finalWindow = new DistributedAggregateWindowState<>(
                functionWindowId.getWindowId(), aggregates.get(functionWindowId));

        aggregates.remove(functionWindowId);
        return finalWindow;
    }

    @Override
    public Integer lowerFinalValue(AggregateWindow finalWindow) {
        throw new RuntimeException(this.getClass().getSimpleName() + " does not support lowerFinalValue()");
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return new ArrayList<>(Collections.singletonList(new HolisticNoopFunction()));
    }
}
