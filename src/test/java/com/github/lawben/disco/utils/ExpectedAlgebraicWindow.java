package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.AlgebraicPartial;
import de.tub.dima.scotty.core.WindowAggregateId;

public class ExpectedAlgebraicWindow extends ExpectedWindow<AlgebraicPartial> {
    public ExpectedAlgebraicWindow(WindowAggregateId windowAggregateId, AlgebraicPartial value, int childId) {
        super(windowAggregateId, value, childId);
    }

}
