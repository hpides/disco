package com.github.lawben.disco;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.List;

public class DistributedChildSlicer<InputType> extends SlicingWindowOperator<InputType> {
    public DistributedChildSlicer(List<Window> windows, List<AggregateFunction> aggFns) {
        super(new MemoryStateFactory());
        for (Window window : windows) {
            this.addWindowAssigner(window);
        }
        for (AggregateFunction aggFn : aggFns) {
            this.addWindowFunction(aggFn);
        }
    }

    public InputType castFromObject(Object item) {
        return (InputType) item;
    }
}
