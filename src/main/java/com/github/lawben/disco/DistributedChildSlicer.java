package com.github.lawben.disco;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.StateFactory;
import java.util.List;

public class DistributedChildSlicer<InputType> extends SlicingWindowOperator<InputType> {


    public DistributedChildSlicer(StateFactory stateFactory) {
        super(stateFactory);
    }

    @Override
    public List<AggregateWindow> processWatermark(long watermarkTs) {
        return this.windowManager.processWatermark(watermarkTs);
    }

    public InputType castFromObject(Object item) {
        return (InputType) item;
    }
}
