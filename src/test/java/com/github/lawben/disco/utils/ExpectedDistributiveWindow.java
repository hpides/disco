package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;

public class ExpectedDistributiveWindow extends ExpectedWindow<Integer> {

    public ExpectedDistributiveWindow(FunctionWindowAggregateId functionWindowAggregateId, Integer value, int childId) {
        super(functionWindowAggregateId, value, childId);
    }
}
