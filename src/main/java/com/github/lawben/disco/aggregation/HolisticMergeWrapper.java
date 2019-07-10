package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.ReduceAggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.ArrayList;
import java.util.List;

// Used in merge process to handle slices.
public class HolisticMergeWrapper implements ReduceAggregateFunction<List<? extends Slice>> {
    private final HolisticAggregateFunction originalFn;

    public HolisticMergeWrapper(HolisticAggregateFunction originalFn) {
        this.originalFn = originalFn;
    }

    public HolisticMergeWrapper() {
        this(null);
    }

    public HolisticAggregateFunction getOriginalFn() {
        return originalFn;
    }

    @Override
    public List<? extends Slice> combine(List<? extends Slice> partialAggregate1, List<? extends Slice> partialAggregate2) {
        List<Slice> copy = new ArrayList<>(partialAggregate1);
        copy.addAll(partialAggregate2);
        return copy;
    }
}
