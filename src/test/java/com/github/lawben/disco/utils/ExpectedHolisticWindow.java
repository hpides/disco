package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.DistributedSlice;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.List;

public class ExpectedHolisticWindow extends ExpectedWindow<List<DistributedSlice>> {
    public ExpectedHolisticWindow(WindowAggregateId windowAggregateId, List<DistributedSlice> value, int childId) {
        super(windowAggregateId, value, childId);
    }
}
