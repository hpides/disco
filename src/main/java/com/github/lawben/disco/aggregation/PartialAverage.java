package com.github.lawben.disco.aggregation;

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
}
