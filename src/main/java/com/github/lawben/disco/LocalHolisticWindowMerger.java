package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticNoopFunction;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateState;
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

public class LocalHolisticWindowMerger extends BaseWindowMerger<List<Slice>> {
    private final StateFactory stateFactory;
    private final Map<Integer, Set<Long>> seenSlices;
    private final Map<FunctionWindowAggregateId, AggregateState<List<Slice>>> aggregates;
    private final Set<Long> sessionWindowIds;

    public LocalHolisticWindowMerger(int numStreams, List<Window> windows) {
        super(numStreams);
        this.stateFactory = new MemoryStateFactory();
        this.seenSlices = new HashMap<>();
        this.aggregates = new HashMap<>();

        this.sessionWindowIds = new HashSet<>();
        for (int windowId = 0; windowId < windows.size(); windowId++) {
            if (windows.get(windowId) instanceof SessionWindow) {
                this.sessionWindowIds.add((long) windowId);
            }
        }
    }

    @Override
    public Optional<FunctionWindowAggregateId> processPreAggregate(List<Slice> preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        final int streamId = functionWindowAggId.getStreamId();
        seenSlices.putIfAbsent(streamId, new HashSet<>());

        Set<Long> seenStreamSlices = seenSlices.get(streamId);
        List<Slice> newSlices = new ArrayList<>(preAggregate.size());

        for (Slice slice : preAggregate) {
            if (seenStreamSlices.contains(slice.getTStart())) {
                continue;
            }

            newSlices.add(slice);
            seenStreamSlices.add(slice.getTStart());
        }

        List<AggregateFunction> dummyFn = Collections.singletonList(new HolisticNoopFunction());
        AggregateState<List<Slice>> windowAgg = new AggregateState<>(this.stateFactory, dummyFn);
        windowAgg.addElement(newSlices);

        aggregates.put(functionWindowAggId, windowAgg);
        return Optional.of(functionWindowAggId);
    }

    @Override
    public DistributedAggregateWindowState<List<Slice>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        AggregateState<List<Slice>> aggState = aggregates.remove(functionWindowId);
        DistributedAggregateWindowState<List<Slice>> windowState =
                new DistributedAggregateWindowState<>(functionWindowId, aggState);

        long windowId = functionWindowId.getWindowId().getWindowId();
        // Session windows are always "complete"
        boolean windowIsComplete = this.sessionWindowIds.contains(windowId) || this.checkWindowTrigger(functionWindowId).isPresent();
        if (windowIsComplete) {
            windowState.setWindowComplete();
        }

        return windowState;
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
