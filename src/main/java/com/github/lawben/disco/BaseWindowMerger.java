package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.FunctionWindowId;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.Window;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public abstract class BaseWindowMerger<AggType> implements WindowMerger<AggType> {
    private int numRemainingChildren;
    protected final Map<FunctionWindowAggregateId, LongAdder> receivedWindows;

    public BaseWindowMerger(int numChildren) {
        this.numRemainingChildren = numChildren;
        this.receivedWindows = new HashMap<>();
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
}
