package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;

public class WindowResult {
    private final FunctionWindowAggregateId finalWindowId;
    private final Long value;

    public WindowResult(FunctionWindowAggregateId finalWindowId, Long value) {
        this.finalWindowId = finalWindowId;
        this.value = value;
    }

    public FunctionWindowAggregateId getFinalWindowId() {
        return finalWindowId;
    }

    public Long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "WindowResult{" +
                "finalWindowId=" + finalWindowId +
                ", value=" + value +
                '}';
    }
}
