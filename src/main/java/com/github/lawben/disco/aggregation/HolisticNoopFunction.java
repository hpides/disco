package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.List;

public class HolisticNoopFunction implements ReduceAggregateFunction<List<Slice>> {
    private final HolisticAggregateFunction originalFn;

    public HolisticNoopFunction(HolisticAggregateFunction originalFn) {
        this.originalFn = originalFn;
    }

    public HolisticNoopFunction() {
        this(null);
    }

    public HolisticAggregateFunction getOriginalFn() {
        return originalFn;
    }

    @Override
    public List<Slice> combine(List<Slice> partialAggregate1, List<Slice> partialAggregate2) {
        throw new RuntimeException("combine not supported");
    }
}
