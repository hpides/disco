package com.github.lawben.disco.aggregation;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.state.AggregateState;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DistributedAggregateWindowState<AggregateType> implements AggregateWindow<AggregateType> {
    private final FunctionWindowAggregateId functionWindowId;
    private final AggregateState<AggregateType> windowState;

    public DistributedAggregateWindowState(FunctionWindowAggregateId functionWindowId, AggregateState<AggregateType> windowState) {
        this.functionWindowId = functionWindowId;
        this.windowState = windowState;
    }

    public FunctionWindowAggregateId getFunctionWindowId() {
        return functionWindowId;
    }

    @Override
    public WindowMeasure getMeasure() {
        return WindowMeasure.Time;
    }

    @Override
    public long getStart() {
        return this.functionWindowId.getWindowId().getWindowStartTimestamp();
    }

    @Override
    public long getEnd() {
        return this.functionWindowId.getWindowId().getWindowEndTimestamp();
    }

    @Override
    public List<AggregateType> getAggValues() {
        return windowState.getValues().stream()
                .map((Object value) -> (AggregateType) value)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public AggregateState<AggregateType> getWindowState() {
        return windowState;
    }

    @Override
    public boolean hasValue() {
        return windowState.hasValues();
    }

    @Override
    public WindowAggregateId getWindowAggregateId() {
        return this.functionWindowId.getWindowId();
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return this.windowState.getAggregateFunctions();
    }

    @Override
    public String toString() {
        return "DistributedAggregateWindowState{" +
                "functionWindowId=" + functionWindowId +
                ", windowState=" + windowState +
                '}';
    }
}
