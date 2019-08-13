package com.github.lawben.disco.merge;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.List;

public class FinalWindowsAndSessionStarts {
    private final List<DistributedAggregateWindowState> finalWindows;
    private final List<FunctionWindowAggregateId> newSessionStarts;

    public FinalWindowsAndSessionStarts(
            List<DistributedAggregateWindowState> finalWindows,
            List<FunctionWindowAggregateId> newSessionStarts) {
        this.finalWindows = finalWindows;
        this.newSessionStarts = newSessionStarts;
    }

    public List<DistributedAggregateWindowState> getFinalWindows() {
        return finalWindows;
    }

    public List<FunctionWindowAggregateId> getNewSessionStarts() {
        return newSessionStarts;
    }
}
