package com.github.lawben.disco.aggregation;

public interface AlgebraicPartial<PartialType, ResultType> {
    ResultType lower();
    PartialType merge(PartialType other);

    AlgebraicPartial<PartialType, ResultType> fromString(String s);
    String asString();
}
