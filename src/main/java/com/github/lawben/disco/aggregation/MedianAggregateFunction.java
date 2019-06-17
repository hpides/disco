package com.github.lawben.disco.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianAggregateFunction<InputType extends Comparable<InputType>> implements NonDecomposableAggregateFunction<InputType, List<InputType>, InputType> {
    @Override
    public List<InputType> lift(InputType inputTuple) {
        List<InputType> lifted = new ArrayList<>();
        lifted.add(inputTuple);
        return lifted;
    }

    @Override
    public List<InputType> combine(List<InputType> partialAggregate1, List<InputType> partialAggregate2) {
        partialAggregate1.addAll(partialAggregate2);
        return partialAggregate1;
    }

    @Override
    public InputType lower(List<InputType> aggregate) {
        assert !aggregate.isEmpty();
        Collections.sort(aggregate);
        return aggregate.get(aggregate.size() / 2);
    }
}
