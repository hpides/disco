package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

public interface HolisticAggregateFunction<InputType, PartialAggregateType, ResultType> extends
        AggregateFunction<InputType, PartialAggregateType, ResultType> {
}
