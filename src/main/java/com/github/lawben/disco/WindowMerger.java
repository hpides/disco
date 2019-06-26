package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import java.util.Optional;

public interface WindowMerger<AggType> {
    Optional<FunctionWindowAggregateId> processPreAggregate(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId);
    AggregateWindow<AggType> triggerFinalWindow(FunctionWindowAggregateId functionWindowId);
}
