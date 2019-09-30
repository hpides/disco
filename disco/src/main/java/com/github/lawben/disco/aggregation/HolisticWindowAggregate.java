package com.github.lawben.disco.aggregation;

import static com.github.lawben.disco.DistributedUtils.slicesToString;
import static com.github.lawben.disco.Event.NO_KEY;

import com.github.lawben.disco.DistributedUtils;
import java.util.List;

public class HolisticWindowAggregate extends BaseWindowAggregate<List<DistributedSlice>> {
    private final int functionId;

    public HolisticWindowAggregate(List<DistributedSlice> value) {
        this(value, NO_KEY);
    }

    public HolisticWindowAggregate(List<DistributedSlice> value, int key) {
        this(value, key, 0);
    }

    public HolisticWindowAggregate(List<DistributedSlice> value, int key, int functionId) {
        super(DistributedUtils.HOLISTIC_STRING, value, key);
        this.functionId = functionId;
    }

    @Override
    public String valueAsString() {
        return slicesToString(this.value, functionId);
    }
}
