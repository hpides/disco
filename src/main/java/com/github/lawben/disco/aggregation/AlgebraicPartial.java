package com.github.lawben.disco.aggregation;

public interface AlgebraicPartial<PartialType, ResultType> {
    ResultType lower();
    PartialType merge(PartialType other);
}
