package com.github.lawben.disco.utils;

import de.tub.dima.scotty.core.WindowAggregateId;

public abstract class ExpectedWindow<T> {
    private final T value;
    private final WindowAggregateId windowAggregateId;
    private final int childId;

    public ExpectedWindow(WindowAggregateId windowAggregateId, T value, int childId) {
        this.windowAggregateId = windowAggregateId;
        this.value = value;
        this.childId = childId;
    }

    public int getChildId() {
        return childId;
    }

    public WindowAggregateId getWindowAggregateId() {
        return windowAggregateId;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ExpectedWindow{" +
                "value=" + value +
                ", windowAggregateId=" + windowAggregateId +
                ", childId=" + childId +
                '}';
    }
}
