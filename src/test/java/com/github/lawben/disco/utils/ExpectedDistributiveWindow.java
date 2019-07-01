package com.github.lawben.disco.utils;

import de.tub.dima.scotty.core.WindowAggregateId;

public class ExpectedDistributiveWindow extends ExpectedWindow<Integer> {

    public ExpectedDistributiveWindow(WindowAggregateId windowAggregateId, Integer value, int childId) {
        super(windowAggregateId, value, childId);
    }
}
