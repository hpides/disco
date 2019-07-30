package com.github.lawben.disco.aggregation;

import java.util.Objects;

public class WindowFunctionId {
    private final long windowId;
    private final int functionId;

    public WindowFunctionId(long windowId, int functionId) {
        this.windowId = windowId;
        this.functionId = functionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowFunctionId that = (WindowFunctionId) o;
        return windowId == that.windowId &&
                functionId == that.functionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, functionId);
    }
}
