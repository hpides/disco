package com.github.lawben.disco;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import java.util.List;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class WindowingConfig {
    private final List<Window> timeWindows;
    private final List<Window> countWindows;
    private final List<AggregateFunction> aggregateFunctions;

    public WindowingConfig(List<Window> timeWindows, List<Window> countWindows, List<AggregateFunction> aggregateFunctions) {
        this.timeWindows = timeWindows;
        this.countWindows = countWindows;
        this.aggregateFunctions = aggregateFunctions;
    }

    public List<Window> getTimeWindows() {
        return timeWindows;
    }

    public List<Window> getCountWindows() {
        return countWindows;
    }

    public List<AggregateFunction> getAggregateFunctions() {
        return aggregateFunctions;
    }
}
