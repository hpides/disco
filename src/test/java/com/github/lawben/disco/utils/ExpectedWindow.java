package com.github.lawben.disco.utils;

import de.tub.dima.scotty.core.WindowAggregateId;

public class ExpectedWindow {
    private final int childId;
    private final WindowAggregateId windowAggregateId;
    private final Integer value;

    public ExpectedWindow(WindowAggregateId windowAggregateId, Integer value, int childId) {
        this.windowAggregateId = windowAggregateId;
        this.value = value;
        this.childId = childId;
    }

    public WindowAggregateId getWindowAggregateId() {
        return windowAggregateId;
    }

    public Integer getValue() {
        return value;
    }

    public int getChildId() {
        return childId;
    }

    @Override
    public String toString() {
        return "ExpectedWindow{" +
                "childId=" + childId +
                ", windowAggregateId=" + windowAggregateId +
                ", value=" + value +
                '}';
    }
}
