package com.github.lawben.disco.aggregation.functions;

import com.github.lawben.disco.aggregation.AlgebraicPartial;
import java.util.Objects;

public class MaxPartialAverage implements AlgebraicPartial<MaxPartialAverage, Long> {
    private final Long sum;
    private final int count;

    public MaxPartialAverage(Long sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public MaxPartialAverage(int sum, int count) {
        this.sum = (long) sum;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public Long getSum() {
        return sum;
    }

    @Override
    public MaxPartialAverage merge(MaxPartialAverage other) {
        if (other.getSum() == null) {
            return new MaxPartialAverage(sum, count);
        }
        if (sum == null) {
            return new MaxPartialAverage(other.getSum(), other.getCount());
        }

        return new MaxPartialAverage(Math.max(sum, other.getSum()), count + other.getCount());
    }

    @Override
    public MaxPartialAverage fromString(String s) {
        if (s == null || s.contains("null")) {
            return new MaxPartialAverage(null, 0);
        }
        String[] parts = s.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("PartialAverage needs exactly 2 values. Got " + parts.length + " values.");
        }
        Long sum = Long.parseLong(parts[0]);
        int count = Integer.parseInt(parts[1]);
        return new MaxPartialAverage(sum, count);
    }

    @Override
    public String asString() {
        return sum + "," + count;
    }

    @Override
    public Long lower() {
        return sum;
    }

    @Override
    public String toString() {
        return "PartialAverage{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaxPartialAverage that = (MaxPartialAverage) o;
        return count == that.count &&
                Objects.equals(sum, that.sum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count);
    }
}
