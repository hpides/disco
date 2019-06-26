package com.github.lawben.disco.utils;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.Collections;
import java.util.List;

public class MockSlice implements Slice {
    private final long tStart;
    private final long tEnd;
    private final List<Integer> values;

    public MockSlice(long tStart, long tEnd, List<Integer> values) {
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.values = values;
    }

    @Override
    public long getTStart() {
        return tStart;
    }

    @Override
    public long getTEnd() {
        return tEnd;
    }

    @Override
    public AggregateState getAggState() {
        List<AggregateFunction> aggFn = Collections.singletonList(new DummyAggregateFunction());
        AggregateState<List<Integer>> state = new AggregateState<>(new MemoryStateFactory(), aggFn);
        state.addElement(this.values);
        return state;
    }

    public List<Integer> getValues() {
        return values;
    }

    // Ignore from here on down

    @Override
    public long getTFirst() {
        return 0;
    }

    @Override
    public long getTLast() {
        return 0;
    }

    @Override
    public void setTStart(long tStart) {}

    @Override
    public void setTEnd(long tEnd) {}

    @Override
    public void merge(Slice otherSlice) {

    }

    @Override
    public Type getType() {
        return null;
    }

    @Override
    public void setType(Type type) {

    }

    @Override
    public void addElement(Object element, long ts) {

    }

    @Override
    public long getCStart() {
        return 0;
    }

    @Override
    public long getCLast() {
        return 0;
    }
}
