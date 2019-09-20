package com.github.lawben.disco.aggregation.functions;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;

public class MaxAverageAggregateFunction implements AlgebraicAggregateFunction<Long, MaxPartialAverage> {
    @Override
    public MaxPartialAverage lift(Long inputTuple) {
        return new MaxPartialAverage(inputTuple, 1);
    }

    @Override
    public MaxPartialAverage combine(MaxPartialAverage partialAggregate1, MaxPartialAverage partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }

    @Override
    public MaxPartialAverage lower(MaxPartialAverage aggregate) {
        return aggregate;
    }

    @Override
    public MaxPartialAverage partialFromString(String partialString) {
        return new MaxPartialAverage(0L, 0).fromString(partialString);
    }
}
