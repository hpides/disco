package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.List;

public class HolisticDummyFunction implements ReduceAggregateFunction<List<Slice>> {
    @Override
    public List<Slice> combine(List<Slice> partialAggregate1, List<Slice> partialAggregate2) {
        throw new RuntimeException("combine not supported for holistic functions");
    }
}
