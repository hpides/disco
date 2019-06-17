package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

class PartialAverage {
    private final Integer sum;
    private final int count;

    @Override
    public String toString() {
        return "PartialAverage{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }

    public PartialAverage(Integer sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public Integer getSum() {
        return sum;
    }

    public PartialAverage merge(PartialAverage other) {
        if (other.getSum() == null) {
            return new PartialAverage(sum, count);
        }
        if (sum == null) {
            return new PartialAverage(other.getSum(), other.getCount());
        }

        return new PartialAverage(sum + other.getSum(), count + other.getCount());
    }
}

public class AverageAggregateFunction implements AggregateFunction<Integer, PartialAverage, Integer> {
    @Override
    public PartialAverage lift(Integer inputTuple) {
        return new PartialAverage(inputTuple, 1);
    }

    @Override
    public PartialAverage combine(PartialAverage partialAggregate1, PartialAverage partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }

    @Override
    public Integer lower(PartialAverage aggregate) {
        if (aggregate.getSum() == null) {
            return null;
        }
        return aggregate.getSum() / aggregate.getCount();
    }
}
