package com.github.lawben.disco.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MedianAggregateFunction implements HolisticAggregateFunction<Integer, List<Integer>, Integer> {
    @Override
    public List<Integer> lift(Integer inputTuple) {
        return new ArrayList<>(Collections.singletonList(inputTuple));
    }

    @Override
    public List<Integer> combine(List<Integer> partialAggregate1, List<Integer> partialAggregate2) {
        return null;
    }

    @Override
    public Integer lower(List<Integer> aggregate) {
        if (aggregate.isEmpty()) {
            return null;
        }

        aggregate.sort(Comparator.naturalOrder());
        return aggregate.get(aggregate.size() / 2);
    }
}
