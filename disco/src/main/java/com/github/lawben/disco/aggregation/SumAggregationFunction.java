package com.github.lawben.disco.aggregation;

public class SumAggregationFunction implements DistributiveAggregateFunction<Integer> {
    @Override
    public Integer lift(Integer inputTuple) {
        return inputTuple;
    }

    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        if (partialAggregate1 == null) return partialAggregate2;
        if (partialAggregate2 == null) return partialAggregate1;
        return partialAggregate1 + partialAggregate2;
    }

    @Override
    public Integer lower(Integer aggregate) {
        return aggregate;
    }
}
