package com.github.lawben.disco;

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
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.Nullable;

public class DistributedWindowMerger<InputType> extends SlicingWindowOperator<InputType> {

    private int numRemainingChildren;
    private final Map<Long, Long> sessionWindowGaps;
    private final Map<Long, WindowAggregateId> currentSessionWindowIds;
    private final Map<WindowAggregateId, LongAdder> receivedWindowPreAggregates = new HashMap<>();
    private final Map<WindowAggregateId, AggregateState<InputType>> windowAggregates = new HashMap<>();

    public DistributedWindowMerger(int numChildren, List<Window> windows, AggregateFunction aggFn) {
        super(new MemoryStateFactory());
        this.numRemainingChildren = numChildren;
        this.addWindowFunction(aggFn);

        this.currentSessionWindowIds = new HashMap<>();
        this.sessionWindowGaps = new HashMap<>();
        for (Window window : windows) {
            this.addWindowAssigner(window);
            if (window instanceof SessionWindow) {
                SessionWindow sw = (SessionWindow) window;
                long windowId = sw.getWindowId();
                this.sessionWindowGaps.put(windowId, sw.getGap());
                this.currentSessionWindowIds.put(windowId, new WindowAggregateId(windowId, -1L, -1L));
            }
        }
    }

    public Optional<WindowAggregateId> processPreAggregate(InputType preAggregate, WindowAggregateId windowAggregateId) {
        final long windowId = windowAggregateId.getWindowId();

        // Process session windows differently
        if (sessionWindowGaps.containsKey(windowId)) {
            return processSessionWindow(preAggregate, windowAggregateId);
        }

        Optional<AggregateState<InputType>> presentAggWindow =
                Optional.ofNullable(windowAggregates.putIfAbsent(windowAggregateId,
                        new AggregateState<>(this.stateFactory, this.windowManager.getAggregations())));

        AggregateState<InputType> aggWindow = presentAggWindow.orElseGet(() -> windowAggregates.get(windowAggregateId));
        aggWindow.addElement(preAggregate);

        LongAdder receivedCounter = receivedWindowPreAggregates.computeIfAbsent(windowAggregateId, k -> new LongAdder());
        if (receivedCounter.longValue() == 0) {
            receivedCounter.add(this.numRemainingChildren);
        }
        receivedCounter.decrement();
        return receivedCounter.longValue() == 0 ? Optional.of(windowAggregateId) : Optional.empty();
    }

    private Optional<WindowAggregateId> processSessionWindow(InputType preAggregate, WindowAggregateId windowAggregateId) {
        final long windowId = windowAggregateId.getWindowId();
        WindowAggregateId windowPlaceholderId = new WindowAggregateId(windowId, 0, 0);
        WindowAggregateId currentWindowId = currentSessionWindowIds.get(windowId);
        long lastTimestamp = currentWindowId.getWindowEndTimestamp();

        if (lastTimestamp == -1L) {
            // There is no session for this window
            AggregateState<InputType> newAggWindow = new AggregateState<>(this.stateFactory, this.windowManager.getAggregations());
            newAggWindow.addElement(preAggregate);
            windowAggregates.put(windowPlaceholderId, newAggWindow);
            currentSessionWindowIds.put(windowId, windowAggregateId);
            return Optional.empty();
        } else {
            // There is a current session for this window
            AggregateState<InputType> aggWindow = windowAggregates.get(windowPlaceholderId);
            final long gap = sessionWindowGaps.get(windowId);

            final long endTimestamp = windowAggregateId.getWindowEndTimestamp();
            if (lastTimestamp + gap > endTimestamp) {
                // This aggregate belongs to the current session
                aggWindow.addElement(preAggregate);

                final long newStartTime = Math.min(currentWindowId.getWindowStartTimestamp(), windowAggregateId.getWindowStartTimestamp());
                final long newEndTime = Math.max(endTimestamp, windowAggregateId.getWindowStartTimestamp());
                WindowAggregateId newCurrentWindowId = new WindowAggregateId(windowId, newStartTime, newEndTime);
                currentSessionWindowIds.put(windowId, newCurrentWindowId);
                return Optional.empty();
            } else {
                // This aggregate starts a new session
                AggregateState<InputType> newAggWindow = new AggregateState<>(this.stateFactory, this.windowManager.getAggregations());
                newAggWindow.addElement(preAggregate);
                windowAggregates.put(windowPlaceholderId, newAggWindow);

                // Trigger window that just finished
                windowAggregates.put(currentWindowId, aggWindow);
                return Optional.of(currentWindowId);
            }
        }
    }

    public AggregateWindow<InputType> triggerFinalWindow(WindowAggregateId windowId) {
        AggregateWindow finalWindow = new DistributedAggregateWindowState<>(windowId, windowAggregates.get(windowId));

        receivedWindowPreAggregates.remove(windowId);
        windowAggregates.remove(windowId);

        return finalWindow;
    }

    public void removeChild() {
        this.numRemainingChildren--;
    }
}
