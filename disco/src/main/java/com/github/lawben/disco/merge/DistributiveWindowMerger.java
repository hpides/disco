package com.github.lawben.disco.merge;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.state.AggregateState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributiveWindowMerger<AggType> extends BaseWindowMerger<AggType> {
    public DistributiveWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        super(numChildren, windows, aggFunctions);
    }

    @Override
    public void processPreAggregate(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        if (this.isSessionWindow(functionWindowAggId)) {
            processGlobalSession(preAggregate, functionWindowAggId);
            return;
        }

        FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
        AggregateFunction aggFn = this.aggFunctions.get(functionWindowAggId.getFunctionId());
        List<AggregateFunction> stateAggFns = Collections.singletonList(aggFn);
        Map<Integer, List<DistributedAggregateWindowState<AggType>>> keyedStates =
                windowAggregates.computeIfAbsent(keylessId, id -> new HashMap<>());

        int key = functionWindowAggId.getKey();
        List<DistributedAggregateWindowState<AggType>> aggWindows =
                keyedStates.computeIfAbsent(key, k -> new ArrayList<>());

        if (aggWindows.isEmpty()) {
            aggWindows.add(new DistributedAggregateWindowState<>(keylessId,
                                new AggregateState<>(this.stateFactory, stateAggFns)));
        }

        DistributedAggregateWindowState<AggType> aggWindow = aggWindows.get(0);
        AggregateState<AggType> state = aggWindow.getWindowState();
        state.addElement(preAggregate);
    }

    @Override
    public Long lowerFinalValue(AggregateWindow finalWindow) {
        List aggValues = finalWindow.getAggValues();
        return aggValues.isEmpty() ? null : (Long) aggValues.get(0);
    }
}
