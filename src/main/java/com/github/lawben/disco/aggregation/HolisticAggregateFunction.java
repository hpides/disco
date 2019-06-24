package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

public interface HolisticAggregateFunction<InputType, PartialAggregateType extends HolisticPartial<PartialAggregateType, InputType>> extends
        AggregateFunction<InputType, PartialAggregateType, PartialAggregateType> {
    @Override
    default PartialAggregateType lower(PartialAggregateType aggregate) {
        return aggregate;
    }

    @Override
    default PartialAggregateType combine(PartialAggregateType partialAggregate1, PartialAggregateType partialAggregate2) {
        return partialAggregate1.merge(partialAggregate2);
    }
}
