package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;

public class ExpectedAlgebraicWindow extends ExpectedWindow<AlgebraicPartial> {
    public ExpectedAlgebraicWindow(FunctionWindowAggregateId functionWindowAggregateId, AlgebraicPartial value, int childId) {
        super(functionWindowAggregateId, value, childId);
    }

}
