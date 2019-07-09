package com.github.lawben.disco.aggregation;

import java.util.Objects;

public class ChildStreamId {
    private final int childId;
    private final int streamId;

    public ChildStreamId(int childId, int streamId) {
        this.childId = childId;
        this.streamId = streamId;
    }

    public int getChildId() {
        return childId;
    }

    public int getStreamId() {
        return streamId;
    }

    public static ChildStreamId fromFunctionWindowId(FunctionWindowAggregateId functionWindowId) {
        final int childId = functionWindowId.getChildId();
        final int streamId = functionWindowId.getStreamId();
        return new ChildStreamId(childId, streamId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChildStreamId that = (ChildStreamId) o;
        return childId == that.childId &&
                streamId == that.streamId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(childId, streamId);
    }

    @Override
    public String toString() {
        return "ChildStreamId{" +
                "childId=" + childId +
                ", streamId=" + streamId +
                '}';
    }
}
