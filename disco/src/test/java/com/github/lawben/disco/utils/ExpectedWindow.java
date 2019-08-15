package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.BaseWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.Arrays;
import java.util.List;

public class ExpectedWindow {
    private final int childId;
    private final FunctionWindowAggregateId functionWindowId;
    private final List<BaseWindowAggregate> expectedWindowAggregates;

    public ExpectedWindow(int childId, FunctionWindowAggregateId functionWindowId, BaseWindowAggregate... expectedWindowAggregates) {
        this(childId, functionWindowId, Arrays.asList(expectedWindowAggregates));
    }

    public ExpectedWindow(int childId, FunctionWindowAggregateId functionWindowId, List<BaseWindowAggregate> expectedWindowAggregates) {
        this.childId = childId;
        this.functionWindowId = functionWindowId;
        this.expectedWindowAggregates = expectedWindowAggregates;
    }

    public int getChildId() {
        return childId;
    }

    public FunctionWindowAggregateId getFunctionWindowId() {
        return functionWindowId;
    }

    public List<BaseWindowAggregate> getExpectedWindowAggregates() {
        return expectedWindowAggregates;
    }

    @Override
    public String toString() {
        return "ExpectedWindow{" +
                "childId=" + childId +
                ", functionWindowId=" + functionWindowId +
                ", expectedWindowAggregates=" + expectedWindowAggregates +
                '}';
    }
}
