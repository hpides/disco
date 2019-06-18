package com.github.lawben.disco.aggregation;

public interface AlgebraicPartial<T> {
    Integer lower();
    T merge(T other);
}
