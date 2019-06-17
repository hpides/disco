package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

public interface NonDecomposableAggregateFunction<InputType, PartialAggregateType, FinalAggregateType> extends
        AggregateFunction<InputType, PartialAggregateType, FinalAggregateType> {}
