package com.github.lawben.disco.aggregation;

public interface HolisticPartial<PartialType, ResultType> {
    ResultType lower();
    PartialType merge(PartialType other);
}
