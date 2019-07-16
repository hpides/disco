package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.List;

public class ExpectedHolisticWindow extends ExpectedWindow<List<DistributedSlice>> {

    public ExpectedHolisticWindow(FunctionWindowAggregateId functionWindowAggregateId, List<DistributedSlice> value, int childId) {
        this(functionWindowAggregateId, value, childId, true);
    }

    public ExpectedHolisticWindow(FunctionWindowAggregateId functionWindowAggregateId, List<DistributedSlice> value, int childId, boolean windowIsComplete) {
        super(functionWindowAggregateId, value, childId);
        this.windowIsComplete = windowIsComplete;
    }
}
