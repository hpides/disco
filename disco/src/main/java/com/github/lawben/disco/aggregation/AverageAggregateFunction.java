package com.github.lawben.disco.aggregation;

public class AverageAggregateFunction implements AlgebraicAggregateFunction<Long, PartialAverage> {
    @Override
    public PartialAverage lift(Long inputTuple) {
        return new PartialAverage(inputTuple, 1);
    }

    @Override
    public PartialAverage combine(PartialAverage partialAggregate1, PartialAverage partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }

    @Override
    public PartialAverage lower(PartialAverage aggregate) {
        return aggregate;
    }

    @Override
    public PartialAverage partialFromString(String partialString) {
        return new PartialAverage(0L, 0).fromString(partialString);
    }
}
