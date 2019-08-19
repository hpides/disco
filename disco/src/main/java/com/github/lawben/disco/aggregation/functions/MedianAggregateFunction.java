package com.github.lawben.disco.aggregation.functions;

import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import java.util.ArrayList;
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
        partialAggregate1.addAll(partialAggregate2);
        return partialAggregate1;
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
