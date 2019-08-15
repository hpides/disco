package com.github.lawben.disco.merge;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.aggregation.WindowFunctionKey;
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
import java.util.Set;

public class LocalHolisticWindowMerger extends BaseWindowMerger<List<Slice>> {
    private final StateFactory stateFactory;
    private final Map<Integer, Set<Long>> seenSlices;
    private final Map<FunctionWindowAggregateId, AggregateState<List<Slice>>> aggregates;
    private final Map<Long, Long> sessionWindowGaps;
    private final Map<WindowFunctionKey, Long> lastTupleTimestamps;

    public LocalHolisticWindowMerger(int numStreams, List<Window> windows) {
        super(numStreams, windows, Collections.singletonList(new HolisticMergeWrapper()));
        this.stateFactory = new MemoryStateFactory();
        this.seenSlices = new HashMap<>();
        this.aggregates = new HashMap<>();
        this.lastTupleTimestamps = new HashMap<>();

        this.sessionWindowGaps = new HashMap<>();
        for (Window window : windows) {
            if (window instanceof SessionWindow) {
                this.sessionWindowGaps.put(window.getWindowId(), ((SessionWindow) window).getGap());
            }
        }
    }

    @Override
    public void processPreAggregate(List<Slice> preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        WindowFunctionKey windowFunctionKey = WindowFunctionKey.fromFunctionWindowId(functionWindowAggId);
        final int key = functionWindowAggId.getKey();
        Set<Long> seenStreamSlices = seenSlices.computeIfAbsent(key, k -> new HashSet<>());
        List<Slice> newSlices = new ArrayList<>(preAggregate.size());
        final long lastTuple = lastTupleTimestamps.computeIfAbsent(windowFunctionKey, k -> 0L);

        long lastTs = 0;
        for (Slice slice : preAggregate) {
            if (seenStreamSlices.contains(slice.getTStart())) {
                continue;
            }

            newSlices.add(slice);
            seenStreamSlices.add(slice.getTStart());
            lastTs = slice.getTLast();
        }
        lastTupleTimestamps.put(windowFunctionKey, lastTs);

        List<AggregateFunction> dummyFn = Collections.singletonList(new HolisticMergeWrapper());
        AggregateState<List<Slice>> windowAgg = new AggregateState<>(this.stateFactory, dummyFn);
        windowAgg.addElement(newSlices);

        final long windowId = functionWindowAggId.getWindowId().getWindowId();
        final long sessionGap = sessionWindowGaps.getOrDefault(windowId, -1L);
        if (sessionGap > 0) {
            // If < 0, this is not a session so we don't care about session starts
            long windowStart = functionWindowAggId.getWindowId().getWindowStartTimestamp();
            if (lastTuple == 0 || lastTuple + sessionGap < windowStart) {
                WindowFunctionKey windowKey = WindowFunctionKey.fromFunctionlessFunctionWindowId(functionWindowAggId);
                newSessionStarts.computeIfAbsent(windowKey, k -> new ArrayList<>()).add(functionWindowAggId);
            }
        }

        aggregates.put(functionWindowAggId, windowAgg);
    }

    @Override
    public List<DistributedAggregateWindowState<List<Slice>>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        AggregateState<List<Slice>> aggState = aggregates.remove(functionWindowId);
        DistributedAggregateWindowState<List<Slice>> windowState =
                new DistributedAggregateWindowState<>(functionWindowId, aggState);

        return Collections.singletonList(windowState);
    }

    @Override
    public Integer lowerFinalValue(AggregateWindow finalWindow) {
        throw new RuntimeException(this.getClass().getSimpleName() + " does not support lowerFinalValue()");
    }
}
