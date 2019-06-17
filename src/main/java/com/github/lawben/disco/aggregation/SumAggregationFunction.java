package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;

public class SumAggregationFunction implements ReduceAggregateFunction<Integer> {
    @Override
    public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
        if (partialAggregate1 == null) return partialAggregate2;
        if (partialAggregate2 == null) return partialAggregate1;
        return partialAggregate1 + partialAggregate2;
    }
}
