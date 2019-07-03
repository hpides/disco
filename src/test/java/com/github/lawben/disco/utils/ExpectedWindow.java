package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;

public abstract class ExpectedWindow<T> {
    private final T value;
    private final FunctionWindowAggregateId functionWindowAggregateId;
    private final int childId;

    public ExpectedWindow(FunctionWindowAggregateId functionWindowAggregateId, T value, int childId) {
        this.functionWindowAggregateId = functionWindowAggregateId;
        this.value = value;
        this.childId = childId;
    }

    public int getChildId() {
        return childId;
    }

    public FunctionWindowAggregateId getFunctionWindowAggregateId() {
        return functionWindowAggregateId;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ExpectedWindow{" +
                "value=" + value +
                ", functionWindowAggregateId=" + functionWindowAggregateId +
                ", childId=" + childId +
                '}';
    }
}
