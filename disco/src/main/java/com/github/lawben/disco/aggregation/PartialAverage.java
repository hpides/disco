package com.github.lawben.disco.aggregation;

import java.util.Objects;

public class PartialAverage implements AlgebraicPartial<PartialAverage, Integer> {
    private final Integer sum;
    private final int count;

    public PartialAverage(Integer sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public Integer getSum() {
        return sum;
    }

    @Override
    public PartialAverage merge(PartialAverage other) {
        if (other.getSum() == null) {
            return new PartialAverage(sum, count);
        }
        if (sum == null) {
            return new PartialAverage(other.getSum(), other.getCount());
        }

        return new PartialAverage(sum + other.getSum(), count + other.getCount());
    }

    @Override
    public PartialAverage fromString(String s) {
        String[] parts = s.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("PartialAverage needs exactly 2 values. Got " + parts.length + " values.");
        }
        int sum = Integer.valueOf(parts[0]);
        int count = Integer.valueOf(parts[1]);
        return new PartialAverage(sum, count);
    }

    @Override
    public String asString() {
        return sum + "," + count;
    }

    @Override
    public Integer lower() {
        if (sum == null) {
            return null;
        }
        return sum / count;
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
        PartialAverage that = (PartialAverage) o;
        return count == that.count &&
                Objects.equals(sum, that.sum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count);
    }
}
