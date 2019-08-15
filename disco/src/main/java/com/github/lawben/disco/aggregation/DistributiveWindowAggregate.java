package com.github.lawben.disco.aggregation;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.BaseWindowAggregate;

public class DistributiveWindowAggregate extends BaseWindowAggregate<Integer> {
    public DistributiveWindowAggregate(Integer value) {
        super(DistributedUtils.DISTRIBUTIVE_STRING, value);
    }

    public DistributiveWindowAggregate(Integer value, int key) {
        super(DistributedUtils.DISTRIBUTIVE_STRING, value, key);
    }

    @Override
    public String valueAsString() {
        return String.valueOf(this.value);
    }
}
