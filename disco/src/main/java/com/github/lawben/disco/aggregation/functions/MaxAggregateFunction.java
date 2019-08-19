package com.github.lawben.disco.aggregation.functions;

import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;

public class MaxAggregateFunction implements DistributiveAggregateFunction<Integer> {
    @Override
    public Integer lift(Integer inputTuple) {
        return inputTuple;
    }

    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        if (partialAggregate1 == null) return partialAggregate2;
        if (partialAggregate2 == null) return partialAggregate1;
        return Math.max(partialAggregate1, partialAggregate2);
    }

    @Override
    public Integer lower(Integer aggregate) {
        return aggregate;
    }
}
