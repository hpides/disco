package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;

public class AlgebraicMergeFunction<PartialType extends AlgebraicPartial<PartialType, ResultType>, ResultType> implements ReduceAggregateFunction<PartialType> {
    @Override
    public PartialType combine(PartialType partialAggregate1, PartialType partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }
}
