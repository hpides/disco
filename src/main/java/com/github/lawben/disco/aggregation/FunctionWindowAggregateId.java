package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.Objects;

public class FunctionWindowAggregateId {
    private final WindowAggregateId windowId;
    private final int functionId;

    public FunctionWindowAggregateId(WindowAggregateId windowId, int functionId) {
        this.windowId = windowId;
        this.functionId = functionId;
    }

    public WindowAggregateId getWindowId() {
        return windowId;
    }

    public int getFunctionId() {
        return functionId;
    }

    @Override
    public String toString() {
        return "FunctionWindowAggregateId{" +
                "windowId=" + windowId +
                ", functionId=" + functionId +
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

        FunctionWindowAggregateId that = (FunctionWindowAggregateId) o;

        if (functionId != that.functionId) {
            return false;
        }
        return windowId.equals(that.windowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, functionId);
    }
}
