package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.FunctionWindowId;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public abstract class BaseWindowMerger<AggType> implements WindowMerger<AggType> {
    protected int numRemainingChildren;
    protected final StateFactory stateFactory;
    protected final List<AggregateFunction> aggFunctions;
    protected final Map<FunctionWindowId, FunctionWindowAggregateId> currentSessionWindowIds;
    protected final Map<FunctionWindowAggregateId, LongAdder> receivedWindows;
    protected final Map<FunctionWindowAggregateId, AggregateState<AggType>> windowAggregates = new HashMap<>();

    public BaseWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        this.numRemainingChildren = numChildren;
        this.stateFactory = new MemoryStateFactory();
        this.aggFunctions = aggFunctions;
        this.receivedWindows = new HashMap<>();
        this.currentSessionWindowIds = this.prepareSessionWindows(windows, aggFunctions);
    }

    protected Optional<FunctionWindowAggregateId> checkWindowTrigger(FunctionWindowAggregateId functionWindowAggId) {
        LongAdder receivedCounter = receivedWindows.computeIfAbsent(functionWindowAggId, k -> new LongAdder());
        if (receivedCounter.longValue() == 0) {
            receivedCounter.add(numRemainingChildren);
        }
        receivedCounter.decrement();
        return receivedCounter.longValue() == 0 ? Optional.of(functionWindowAggId) : Optional.empty();
    }

    protected Map<FunctionWindowId, FunctionWindowAggregateId> prepareSessionWindows(List<Window> windows, List<AggregateFunction> aggregateFunctions) {
        Map<FunctionWindowId, FunctionWindowAggregateId> currentSessionWindowIds = new HashMap<>();
        for (Window window : windows) {
            if (window instanceof SessionWindow) {
                SessionWindow sw = (SessionWindow) window;
                long windowId = sw.getWindowId();
                WindowAggregateId dummyId = new WindowAggregateId(windowId, -1L, -1L);

                for (int functionId = 0; functionId < aggregateFunctions.size(); functionId++) {
                    FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);
                    currentSessionWindowIds.put(functionWindowId, new FunctionWindowAggregateId(dummyId, functionId));
                }
            }
        }
        return currentSessionWindowIds;
    }

    protected Optional<FunctionWindowAggregateId> processSessionWindow(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        final WindowAggregateId windowAggId = functionWindowAggId.getWindowId();
        final long windowId = windowAggId.getWindowId();
        final int functionId = functionWindowAggId.getFunctionId();

        FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);
        WindowAggregateId windowPlaceholderId = new WindowAggregateId(windowId, 0, 0);
        FunctionWindowAggregateId functionWindowPlaceholderId =
                new FunctionWindowAggregateId(windowPlaceholderId, functionId);

        FunctionWindowAggregateId currentFunctionWindowId = currentSessionWindowIds.get(functionWindowId);
        final long lastTimestamp = currentFunctionWindowId.getWindowId().getWindowEndTimestamp();

        if (lastTimestamp == -1L) {
            // There is no session for this window
            createNewSession(preAggregate, functionWindowAggId, functionWindowId, functionWindowPlaceholderId);
            return Optional.empty();
        } else {
            // There is a current session for this window
            AggregateState<AggType> aggWindow = windowAggregates.get(functionWindowPlaceholderId);

            final long endTimestamp = windowAggId.getWindowEndTimestamp();
            final long startTimestamp = windowAggId.getWindowStartTimestamp();
            if (startTimestamp < lastTimestamp) {
                // This aggregate belongs to the current session
                aggWindow.addElement(preAggregate);

                final WindowAggregateId currentWindowAggId = currentFunctionWindowId.getWindowId();
                final long newStartTime = Math.min(currentWindowAggId.getWindowStartTimestamp(), startTimestamp);
                final long newEndTime = Math.max(currentWindowAggId.getWindowEndTimestamp(), endTimestamp);
                WindowAggregateId newCurrentWindowId = new WindowAggregateId(windowId, newStartTime, newEndTime);
                FunctionWindowAggregateId newCurrentFunctionWindowId =
                        new FunctionWindowAggregateId(newCurrentWindowId, functionId);
                currentSessionWindowIds.put(functionWindowId, newCurrentFunctionWindowId);
                return Optional.empty();
            } else {
                // This aggregate starts a new session
                createNewSession(preAggregate, functionWindowAggId, functionWindowId, functionWindowPlaceholderId);

                // Trigger window that just finished
                windowAggregates.put(currentFunctionWindowId, aggWindow);
                return Optional.of(currentFunctionWindowId);
            }
        }
    }

    protected boolean isSessionWindow(FunctionWindowAggregateId functionWindowAggId) {
        final long windowId = functionWindowAggId.getWindowId().getWindowId();
        final int functionId = functionWindowAggId.getFunctionId();
        final FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);
        return currentSessionWindowIds.containsKey(functionWindowId);
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return this.aggFunctions;
    }

    private void createNewSession(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId,
            FunctionWindowId functionWindowId, FunctionWindowAggregateId functionWindowPlaceholderId) {
        AggregateFunction aggFn = this.aggFunctions.get(functionWindowAggId.getFunctionId());
        List<AggregateFunction> stateAggFns = Collections.singletonList(aggFn);
        AggregateState<AggType> newAggWindow = new AggregateState<>(this.stateFactory, stateAggFns);
        newAggWindow.addElement(preAggregate);
        windowAggregates.put(functionWindowPlaceholderId, newAggWindow);
        currentSessionWindowIds.put(functionWindowId, functionWindowAggId);
    }
}
