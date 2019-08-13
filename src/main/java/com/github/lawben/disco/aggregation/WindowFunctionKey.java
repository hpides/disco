package com.github.lawben.disco.aggregation;

import java.util.Objects;

public class WindowFunctionKey {
    private final long windowId;
    private final int functionId;
    private final int key;

    public WindowFunctionKey(long windowId, int functionId, int key) {
        this.windowId = windowId;
        this.functionId = functionId;
        this.key = key;
    }

    public WindowFunctionKey(long windowId, int key) {
        this(windowId, 0, key);
    }

    public static WindowFunctionKey fromFunctionWindowId(FunctionWindowAggregateId functionWindowId) {
        return new WindowFunctionKey(functionWindowId.getWindowId().getWindowId(),
                                     functionWindowId.getFunctionId(),
                                     functionWindowId.getKey());
    }

    public static WindowFunctionKey fromFunctionlessFunctionWindowId(FunctionWindowAggregateId functionWindowId) {
        return new WindowFunctionKey(functionWindowId.getWindowId().getWindowId(), 0, functionWindowId.getKey());
    }

    public long getWindowId() {
        return windowId;
    }

    public int getFunctionId() {
        return functionId;
    }

    public int getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "WindowFunctionKey{" +
                "windowId=" + windowId +
                ", functionId=" + functionId +
                ", key=" + key +
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
        WindowFunctionKey that = (WindowFunctionKey) o;
        return windowId == that.windowId &&
                functionId == that.functionId &&
                key == that.key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, functionId, key);
    }
}
