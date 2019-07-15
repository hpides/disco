package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;

public class WindowResult {
    private final FunctionWindowAggregateId finalWindowId;
    private final Integer value;

    public WindowResult(FunctionWindowAggregateId finalWindowId, Integer value) {
        this.finalWindowId = finalWindowId;
        this.value = value;
    }

    public FunctionWindowAggregateId getFinalWindowId() {
        return finalWindowId;
    }

    public Integer getValue() {
        return value;
    }
}
