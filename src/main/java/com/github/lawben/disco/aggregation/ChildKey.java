package com.github.lawben.disco.aggregation;

import java.util.Objects;

public class ChildKey {
    private final int childId;
    private final int key;

    public ChildKey(int childId, int key) {
        this.childId = childId;
        this.key = key;
    }

    public int getChildId() {
        return childId;
    }

    public int getKey() {
        return key;
    }

    public static ChildKey fromFunctionWindowId(FunctionWindowAggregateId functionWindowId) {
        final int childId = functionWindowId.getChildId();
        final int key = functionWindowId.getKey();
        return new ChildKey(childId, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChildKey that = (ChildKey) o;
        return childId == that.childId &&
                key == that.key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(childId, key);
    }

    @Override
    public String toString() {
        return "ChildKey{" +
                "childId=" + childId +
                ", key=" + key +
                '}';
    }
}
