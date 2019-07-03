package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.List;

public class ExpectedHolisticWindow extends ExpectedWindow<List<DistributedSlice>> {
    public ExpectedHolisticWindow(FunctionWindowAggregateId functionWindowAggregateId, List<DistributedSlice> value, int childId) {
        super(functionWindowAggregateId, value, childId);
    }
}
