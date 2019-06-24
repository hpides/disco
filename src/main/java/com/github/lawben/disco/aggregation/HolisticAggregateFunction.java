package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import java.util.Collection;

public interface HolisticAggregateFunction<InputType, PartialAggregateType extends Collection<InputType>> extends
        AggregateFunction<InputType, PartialAggregateType, PartialAggregateType> {
    @Override
    default PartialAggregateType lower(PartialAggregateType aggregate) {
        return aggregate;
    }
}
