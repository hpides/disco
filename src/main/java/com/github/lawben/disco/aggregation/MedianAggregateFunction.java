package com.github.lawben.disco.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MedianAggregateFunction implements HolisticAggregateFunction<Integer, PartialMedian> {
    @Override
    public PartialMedian lift(Integer inputTuple) {
        return new PartialMedian(new ArrayList<>(Collections.singletonList(inputTuple)));
    }
}
