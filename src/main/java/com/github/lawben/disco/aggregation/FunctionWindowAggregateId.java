package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.Objects;

public class FunctionWindowAggregateId {
    public final static int NO_CHILD_ID = -1;

    private final WindowAggregateId windowId;
    private final int functionId;
    private final int childId;

    public FunctionWindowAggregateId(WindowAggregateId windowId, int functionId) {
        this(windowId, functionId, NO_CHILD_ID);
    }

    public FunctionWindowAggregateId(WindowAggregateId windowId, int functionId, int childId) {
        this.windowId = windowId;
        this.functionId = functionId;
        this.childId = childId;
    }

    public FunctionWindowAggregateId(FunctionWindowAggregateId functionWindowAggregateId, int childId) {
        this(functionWindowAggregateId.getWindowId(), functionWindowAggregateId.getFunctionId(), childId);
    }

    public WindowAggregateId getWindowId() {
        return windowId;
    }

    public int getFunctionId() {
        return functionId;
    }

    public int getChildId() {
        return childId;
    }

    @Override
    public String toString() {
        return "FunctionWindowAggregateId{" +
                "windowId=" + windowId +
                ", functionId=" + functionId +
                ", (childId=" + childId +
                ")}";
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
        return functionId == that.functionId &&
                Objects.equals(windowId, that.windowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, functionId);
    }
}
