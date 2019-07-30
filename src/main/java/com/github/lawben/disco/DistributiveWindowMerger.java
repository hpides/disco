package com.github.lawben.disco;

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
//        if (this.isSessionWindow(functionWindowAggId)) {
//            return processSessionWindow(preAggregate, functionWindowAggId);
//        }

        FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
        AggregateFunction aggFn = this.aggFunctions.get(functionWindowAggId.getFunctionId());
        List<AggregateFunction> stateAggFns = Collections.singletonList(aggFn);
        Map<Integer, AggregateState<AggType>> keyedStates =
                windowAggregates.computeIfAbsent(keylessId, id -> new HashMap<>());

        int key = functionWindowAggId.getKey();
        AggregateState<AggType> aggWindow =
                keyedStates.computeIfAbsent(key, k -> new AggregateState<>(this.stateFactory, stateAggFns));
        aggWindow.addElement(preAggregate);
    }

    @Override
    public List<DistributedAggregateWindowState<AggType>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        FunctionWindowAggregateId keylessId = functionWindowId.keylessCopy();
        Map<Integer, AggregateState<AggType>> keyedStates = windowAggregates.remove(keylessId);

        List<DistributedAggregateWindowState<AggType>> finalWindows = new ArrayList<>(keyedStates.size());
        for (Map.Entry<Integer, AggregateState<AggType>> keyedState : keyedStates.entrySet()) {
            int key = keyedState.getKey();
            AggregateState<AggType> state = keyedState.getValue();
            FunctionWindowAggregateId keyedId = keylessId.withKey(key);
            finalWindows.add(new DistributedAggregateWindowState<>(keyedId, state));
        }

        receivedWindows.remove(keylessId);
        return finalWindows;
    }

    @Override
    public Integer lowerFinalValue(AggregateWindow finalWindow) {
        List aggValues = finalWindow.getAggValues();
        return aggValues.isEmpty() ? null : (Integer) aggValues.get(0);
    }
}
