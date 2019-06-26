package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;

public class WindowMergerTestBase {
    protected List<Window> windows;
    protected Window tumblingWindow;
    protected List<AggregateFunction> aggregateFunctions;
    protected MemoryStateFactory stateFactory;

    @BeforeEach
    public void setup() {
        this.windows = new ArrayList<>();
        this.tumblingWindow = new TumblingWindow(WindowMeasure.Time, 1000, 1);
        this.aggregateFunctions = new ArrayList<>();
        this.stateFactory = new MemoryStateFactory();
    }

    protected FunctionWindowAggregateId defaultFnWindowAggId(WindowAggregateId windowAggregateId) {
        return new FunctionWindowAggregateId(windowAggregateId, 0);
    }
}
