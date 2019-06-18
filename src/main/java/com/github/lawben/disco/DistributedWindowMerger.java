package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.state.DistributedAggregateWindowState;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.Nullable;

public class DistributedWindowMerger<InputType> extends SlicingWindowOperator<InputType> {

    private int numRemainingChildren;
    private final Map<FunctionWindowId, FunctionWindowAggregateId> currentSessionWindowIds;
    private final Map<FunctionWindowAggregateId, LongAdder> receivedWindowPreAggregates = new HashMap<>();
    private final Map<FunctionWindowAggregateId, AggregateState<InputType>> windowAggregates = new HashMap<>();

    public DistributedWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        super(new MemoryStateFactory());
        this.numRemainingChildren = numChildren;

        for (AggregateFunction aggFn : aggFunctions) {
            this.addWindowFunction(aggFn);
        }

        this.currentSessionWindowIds = new HashMap<>();
        for (Window window : windows) {
            this.addWindowAssigner(window);
            if (window instanceof SessionWindow) {
                SessionWindow sw = (SessionWindow) window;
                long windowId = sw.getWindowId();
                WindowAggregateId dummyId = new WindowAggregateId(windowId, -1L, -1L);

                for (int functionId = 0; functionId < aggFunctions.size(); functionId++) {
                    FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);
                    this.currentSessionWindowIds.put(functionWindowId, new FunctionWindowAggregateId(dummyId, functionId));
                }
            }
        }
    }

    public Optional<FunctionWindowAggregateId> processPreAggregate(InputType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        final long windowId = functionWindowAggId.getWindowId().getWindowId();
        final int functionId = functionWindowAggId.getFunctionId();
        final FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);

        // Process session windows differently
        if (currentSessionWindowIds.containsKey(functionWindowId)) {
            return processSessionWindow(preAggregate, functionWindowAggId);
        }

        Optional<AggregateState<InputType>> presentAggWindow =
                Optional.ofNullable(windowAggregates.putIfAbsent(functionWindowAggId,
                        new AggregateState<>(this.stateFactory, this.windowManager.getAggregations())));

        AggregateState<InputType> aggWindow = presentAggWindow.orElseGet(() -> windowAggregates.get(functionWindowAggId));
        aggWindow.addElement(preAggregate);

        LongAdder receivedCounter = receivedWindowPreAggregates.computeIfAbsent(functionWindowAggId, k -> new LongAdder());
        if (receivedCounter.longValue() == 0) {
            receivedCounter.add(this.numRemainingChildren);
        }
        receivedCounter.decrement();
        return receivedCounter.longValue() == 0 ? Optional.of(functionWindowAggId) : Optional.empty();
    }

    private Optional<FunctionWindowAggregateId> processSessionWindow(InputType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        final WindowAggregateId windowAggId = functionWindowAggId.getWindowId();
        final long windowId = windowAggId.getWindowId();
        final int functionId = functionWindowAggId.getFunctionId();

        FunctionWindowId functionWindowId = new FunctionWindowId(windowId, functionId);
        WindowAggregateId windowPlaceholderId = new WindowAggregateId(windowId, 0, 0);
        FunctionWindowAggregateId functionWindowPlaceholderId = new FunctionWindowAggregateId(windowPlaceholderId, functionId);

        FunctionWindowAggregateId currentFunctionWindowId = currentSessionWindowIds.get(functionWindowId);
        final long lastTimestamp = currentFunctionWindowId.getWindowId().getWindowEndTimestamp();

        if (lastTimestamp == -1L) {
            // There is no session for this window
            AggregateState<InputType> newAggWindow = new AggregateState<>(this.stateFactory, this.windowManager.getAggregations());
            newAggWindow.addElement(preAggregate);
            windowAggregates.put(functionWindowPlaceholderId, newAggWindow);
            currentSessionWindowIds.put(functionWindowId, functionWindowAggId);
            return Optional.empty();
        } else {
            // There is a current session for this window
            AggregateState<InputType> aggWindow = windowAggregates.get(functionWindowPlaceholderId);

            final long endTimestamp = windowAggId.getWindowEndTimestamp();
            final long startTimestamp = windowAggId.getWindowStartTimestamp();
            if (startTimestamp < lastTimestamp) {
                // This aggregate belongs to the current session
                aggWindow.addElement(preAggregate);

                final WindowAggregateId currentWindowAggId = currentFunctionWindowId.getWindowId();
                final long newStartTime = Math.min(currentWindowAggId.getWindowStartTimestamp(), startTimestamp);
                final long newEndTime = Math.max(endTimestamp, windowAggId.getWindowStartTimestamp());
                WindowAggregateId newCurrentWindowId = new WindowAggregateId(windowId, newStartTime, newEndTime);
                FunctionWindowAggregateId newCurrentFunctionWindowId = new FunctionWindowAggregateId(newCurrentWindowId, functionId);
                currentSessionWindowIds.put(functionWindowId, newCurrentFunctionWindowId);
                return Optional.empty();
            } else {
                // This aggregate starts a new session
                AggregateState<InputType> newAggWindow = new AggregateState<>(this.stateFactory, this.windowManager.getAggregations());
                newAggWindow.addElement(preAggregate);
                windowAggregates.put(functionWindowPlaceholderId, newAggWindow);
                currentSessionWindowIds.put(functionWindowId, functionWindowAggId);

                // Trigger window that just finished
                windowAggregates.put(currentFunctionWindowId, aggWindow);
                return Optional.of(currentFunctionWindowId);
            }
        }
    }

    public AggregateWindow<InputType> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        AggregateWindow<InputType> finalWindow = new DistributedAggregateWindowState<>(
                functionWindowId.getWindowId(), windowAggregates.get(functionWindowId));

        receivedWindowPreAggregates.remove(functionWindowId);
        windowAggregates.remove(functionWindowId);

        return finalWindow;
    }

    void removeChild() {
        this.numRemainingChildren--;
    }

    class FunctionWindowId {
        final long windowId;
        final int functionId;

        FunctionWindowId(long windowId, int functionId) {
            this.windowId = windowId;
            this.functionId = functionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionWindowId that = (FunctionWindowId) o;
            return windowId == that.windowId &&
                    functionId == that.functionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(windowId, functionId);
        }
    }
}
