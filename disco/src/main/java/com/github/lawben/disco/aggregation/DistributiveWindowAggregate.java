package com.github.lawben.disco.aggregation;

import com.github.lawben.disco.DistributedUtils;

public class DistributiveWindowAggregate extends BaseWindowAggregate<Long> {
    public DistributiveWindowAggregate(Long value) {
        super(DistributedUtils.DISTRIBUTIVE_STRING, value);
    }

    public DistributiveWindowAggregate(Long value, int key) {
        super(DistributedUtils.DISTRIBUTIVE_STRING, value, key);
    }

    public DistributiveWindowAggregate(Integer value) {
        super(DistributedUtils.DISTRIBUTIVE_STRING, (long) value);
    }

    public DistributiveWindowAggregate(Integer value, int key) {
        super(DistributedUtils.DISTRIBUTIVE_STRING, (long) value, key);
    }

    @Override
    public String valueAsString() {
        return String.valueOf(this.value);
    }
}
