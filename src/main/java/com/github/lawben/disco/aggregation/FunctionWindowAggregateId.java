package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.Objects;

public class FunctionWindowAggregateId {
    public final static int NO_CHILD_ID = -1;
    public final static int NO_STREAM_ID = -2;

    private final WindowAggregateId windowId;
    private final int functionId;
    private final int childId;
    private final int streamId;

    public FunctionWindowAggregateId(WindowAggregateId windowId, int functionId) {
        this(windowId, functionId, NO_CHILD_ID);
    }

    public FunctionWindowAggregateId(WindowAggregateId windowId, int functionId, int childId) {
        this(windowId, functionId, childId, NO_STREAM_ID);
    }

    public FunctionWindowAggregateId(WindowAggregateId windowId, int functionId, int childId, int streamId) {
        this.windowId = windowId;
        this.functionId = functionId;
        this.childId = childId;
        this.streamId = streamId;
    }

    public FunctionWindowAggregateId(FunctionWindowAggregateId functionWindowAggregateId, int childId) {
        this(functionWindowAggregateId, childId, NO_STREAM_ID);
    }

    public FunctionWindowAggregateId(FunctionWindowAggregateId functionWindowAggregateId, int childId, int streamId) {
        this(functionWindowAggregateId.getWindowId(), functionWindowAggregateId.getFunctionId(), childId, streamId);
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

    public int getStreamId() {
        return streamId;
    }

    @Override
    public String toString() {
        return "FunctionWindowAggregateId{" +
                "windowId=" + windowId +
                ", functionId=" + functionId +
                ", (childId=" + childId + ")" +
                ", (streamId=" + streamId + ")" +
                "}";
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
