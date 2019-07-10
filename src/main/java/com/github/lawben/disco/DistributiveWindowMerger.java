package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.state.AggregateState;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DistributiveWindowMerger<AggType> extends BaseWindowMerger<AggType> {
    public DistributiveWindowMerger(int numChildren, List<Window> windows, List<AggregateFunction> aggFunctions) {
        super(numChildren, windows, aggFunctions);
    }

    @Override
    public Optional<FunctionWindowAggregateId> processPreAggregate(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        if (this.isSessionWindow(functionWindowAggId)) {
            return processSessionWindow(preAggregate, functionWindowAggId);
        }

        AggregateFunction aggFn = this.aggFunctions.get(functionWindowAggId.getFunctionId());
        List<AggregateFunction> stateAggFns = Collections.singletonList(aggFn);
        windowAggregates.putIfAbsent(functionWindowAggId, new AggregateState<>(this.stateFactory, stateAggFns));

        AggregateState<AggType> aggWindow = windowAggregates.get(functionWindowAggId);
        aggWindow.addElement(preAggregate);

        return checkWindowTrigger(functionWindowAggId);
    }

    @Override
    public DistributedAggregateWindowState<AggType> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        DistributedAggregateWindowState<AggType> finalWindow =
                new DistributedAggregateWindowState<>(functionWindowId, windowAggregates.get(functionWindowId));

        receivedWindows.remove(functionWindowId);
        windowAggregates.remove(functionWindowId);

        finalWindow.setWindowComplete();
        return finalWindow;
    }

    @Override
    public Integer lowerFinalValue(AggregateWindow finalWindow) {
        List aggValues = finalWindow.getAggValues();
        return aggValues.isEmpty() ? null : (Integer) aggValues.get(0);
    }
}
