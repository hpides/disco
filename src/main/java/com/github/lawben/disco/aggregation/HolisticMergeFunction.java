package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

public class HolisticMergeFunction<PartialType extends HolisticPartial<PartialType, ResultType>, ResultType> implements AggregateFunction<PartialType, PartialType, ResultType> {
    @Override
    public PartialType lift(PartialType inputTuple) {
        return inputTuple;
    }

    @Override
    public PartialType combine(PartialType partialAggregate1, PartialType partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }

    @Override
    public ResultType lower(PartialType aggregate) {
        return aggregate.lower();
    }
}
