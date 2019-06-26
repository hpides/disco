package com.github.lawben.disco.utils;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;

public class DummyAggregateFunction<T> implements ReduceAggregateFunction<T> {
    @Override
    public T combine(T partialAggregate1, T partialAggregate2) {
        return null;
    }
}
