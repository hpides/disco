package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.WindowFunctionId;
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
    protected int numInputs;
    protected final StateFactory stateFactory;
    protected final List<AggregateFunction> aggFunctions;
    protected final Map<WindowFunctionId, FunctionWindowAggregateId> currentSessionWindowIds;
    protected final Map<FunctionWindowAggregateId, LongAdder> receivedWindows;
    protected final Map<FunctionWindowAggregateId, Map<Integer, AggregateState<AggType>>> windowAggregates;

    public BaseWindowMerger(int numInputs, List<Window> windows, List<AggregateFunction> aggFunctions) {
        this.numInputs = numInputs;
        this.stateFactory = new MemoryStateFactory();
        this.aggFunctions = aggFunctions;
        this.receivedWindows = new HashMap<>();
        this.currentSessionWindowIds = this.prepareSessionWindows(windows, aggFunctions);
        this.windowAggregates = new HashMap<>();
    }

    @Override
    public Optional<FunctionWindowAggregateId> checkWindowComplete(FunctionWindowAggregateId functionWindowAggId) {
        FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();

        LongAdder receivedCounter = receivedWindows.computeIfAbsent(keylessId, k -> new LongAdder());
        if (receivedCounter.longValue() == 0) {
            receivedCounter.add(numInputs);
        }
        receivedCounter.decrement();
        return receivedCounter.longValue() == 0 ? Optional.of(keylessId) : Optional.empty();
    }

    protected Map<WindowFunctionId, FunctionWindowAggregateId> prepareSessionWindows(List<Window> windows, List<AggregateFunction> aggregateFunctions) {
        Map<WindowFunctionId, FunctionWindowAggregateId> currentSessionWindowIds = new HashMap<>();
        for (Window window : windows) {
            if (window instanceof SessionWindow) {
                SessionWindow sw = (SessionWindow) window;
                long windowId = sw.getWindowId();
                WindowAggregateId dummyId = new WindowAggregateId(windowId, -1L, -1L);

                for (int functionId = 0; functionId < aggregateFunctions.size(); functionId++) {
                    WindowFunctionId windowFunctionId = new WindowFunctionId(windowId, functionId);
                    currentSessionWindowIds.put(windowFunctionId, new FunctionWindowAggregateId(dummyId, functionId));
                }
            }
        }
        return currentSessionWindowIds;
    }

    protected void processSessionWindow(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
//        final WindowAggregateId windowAggId = functionWindowAggId.getWindowId();
//        final long windowId = windowAggId.getWindowId();
//        final int functionId = functionWindowAggId.getFunctionId();
//
//        WindowFunctionId windowFunctionId = new WindowFunctionId(windowId, functionId);
//        WindowAggregateId windowPlaceholderId = new WindowAggregateId(windowId, 0, 0);
//        FunctionWindowAggregateId functionWindowPlaceholderId =
//                new FunctionWindowAggregateId(windowPlaceholderId, functionId);
//
//        FunctionWindowAggregateId currentFunctionWindowId = currentSessionWindowIds.get(windowFunctionId);
//        final long lastTimestamp = currentFunctionWindowId.getWindowId().getWindowEndTimestamp();
//
////        SESSION DOES NOT WORK LIKE THIS WITH KEYS. REMOVE NEARLY ALL OF CHILDMERGER!
//        if (lastTimestamp == -1L) {
//            // There is no session for this window
//            createNewSession(preAggregate, functionWindowAggId, windowFunctionId, functionWindowPlaceholderId);
//            return Optional.empty();
//        } else {
//            // There is a current session for this window
//            AggregateState<AggType> aggWindow = windowAggregates.get(functionWindowPlaceholderId);
//
//            final long endTimestamp = windowAggId.getWindowEndTimestamp();
//            final long startTimestamp = windowAggId.getWindowStartTimestamp();
//            if (startTimestamp < lastTimestamp) {
//                // This aggregate belongs to the current session
//                aggWindow.addElement(preAggregate);
//
//                final WindowAggregateId currentWindowAggId = currentFunctionWindowId.getWindowId();
//                final long newStartTime = Math.min(currentWindowAggId.getWindowStartTimestamp(), startTimestamp);
//                final long newEndTime = Math.max(currentWindowAggId.getWindowEndTimestamp(), endTimestamp);
//                WindowAggregateId newCurrentWindowId = new WindowAggregateId(windowId, newStartTime, newEndTime);
//                FunctionWindowAggregateId newCurrentFunctionWindowId =
//                        new FunctionWindowAggregateId(newCurrentWindowId, functionId);
//                currentSessionWindowIds.put(windowFunctionId, newCurrentFunctionWindowId);
//                return Optional.empty();
//            } else {
//                // This aggregate starts a new session
//                createNewSession(preAggregate, functionWindowAggId, windowFunctionId, functionWindowPlaceholderId);
//
//                // Trigger window that just finished
//                windowAggregates.put(currentFunctionWindowId, aggWindow);
//                return Optional.of(currentFunctionWindowId);
//            }
//        }
    }

    protected boolean isSessionWindow(FunctionWindowAggregateId functionWindowAggId) {
        final long windowId = functionWindowAggId.getWindowId().getWindowId();
        final int functionId = functionWindowAggId.getFunctionId();
        final WindowFunctionId windowFunctionId = new WindowFunctionId(windowId, functionId);
        return currentSessionWindowIds.containsKey(windowFunctionId);
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return this.aggFunctions;
    }

    private void createNewSession(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId,
            WindowFunctionId windowFunctionId, FunctionWindowAggregateId functionWindowPlaceholderId) {
//        AggregateFunction aggFn = this.aggFunctions.get(functionWindowAggId.getFunctionId());
//        List<AggregateFunction> stateAggFns = Collections.singletonList(aggFn);
//        AggregateState<AggType> newAggWindow = new AggregateState<>(this.stateFactory, stateAggFns);
//        newAggWindow.addElement(preAggregate);
//        windowAggregates.put(functionWindowPlaceholderId, newAggWindow);
//        currentSessionWindowIds.put(windowFunctionId, functionWindowAggId);
    }
}
