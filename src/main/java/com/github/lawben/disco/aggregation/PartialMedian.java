package com.github.lawben.disco.aggregation;

import java.util.Comparator;
import java.util.List;

public class PartialMedian implements HolisticPartial<PartialMedian, Integer> {
    private final List<Integer> values;

    public PartialMedian(List<Integer> values) {
        this.values = values;
    }

    @Override
    public Integer lower() {
        if (values.isEmpty()) {
            return null;
        }

        values.sort(Comparator.naturalOrder());
        return values.get(values.size() / 2);
    }

    @Override
    public PartialMedian merge(PartialMedian other) {
        values.addAll(other.values);
        return this;
    }
}
