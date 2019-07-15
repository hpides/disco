package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateHelper;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateWindowState;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ChildMerger {
    private final int childId;
    private final Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream;
    private DistributiveWindowMerger<Integer> distributiveWindowMerger;
    private AlgebraicWindowMerger<AlgebraicPartial> algebraicWindowMerger;
    private LocalHolisticWindowMerger localHolisticWindowMerger;

    public ChildMerger(Map<Integer, DistributedChildSlicer<Integer>> slicerPerStream,
            List<Window> timedWindows, List<AggregateFunction> functions, int childId) {
        this.childId = childId;
        this.slicerPerStream = slicerPerStream;

        int numStreams = slicerPerStream.size();
        List<AggregateFunction> stateAggFunctions = DistributedUtils.convertAggregateFunctions(functions);
        this.distributiveWindowMerger = new DistributiveWindowMerger<>(numStreams, timedWindows, stateAggFunctions);
        this.algebraicWindowMerger = new AlgebraicWindowMerger<>(numStreams, timedWindows, stateAggFunctions);
        this.localHolisticWindowMerger = new LocalHolisticWindowMerger(numStreams, timedWindows);
    }

    protected void processElement(int eventValue, long eventTimestamp, int streamId) {
        DistributedChildSlicer<Integer> perStreamSlicer = this.slicerPerStream.get(streamId);
        perStreamSlicer.processElement(eventValue, eventTimestamp);
    }

    protected List<DistributedAggregateWindowState> processWatermarkedWindows(long watermarkTimestamp) {
        return this.slicerPerStream.entrySet().stream().flatMap((var slicerWithId) -> {
            int streamId = slicerWithId.getKey();
            DistributedChildSlicer<Integer> slicer = slicerWithId.getValue();
            List<AggregateWindow> preAggregatedWindows = slicer.processWatermark(watermarkTimestamp);
            List<DistributedAggregateWindowState> finalWindows = this.mergeStreamWindows(preAggregatedWindows, streamId);
            return finalWindows.stream();
        }).collect(Collectors.toList());
    }

    private List<DistributedAggregateWindowState> mergeStreamWindows(List<AggregateWindow> preAggregatedWindows, int streamId) {
        List<DistributedAggregateWindowState> finalPreAggregateWindows = new ArrayList<>(preAggregatedWindows.size());

        preAggregatedWindows.sort(Comparator.comparingLong(AggregateWindow::getStart));
        for (AggregateWindow preAggWindow : preAggregatedWindows) {
            List<DistributedAggregateWindowState> aggregateWindows = mergePreAggWindow((AggregateWindowState) preAggWindow, streamId);
            finalPreAggregateWindows.addAll(aggregateWindows);
        }

        return finalPreAggregateWindows;
    }

    private List<DistributedAggregateWindowState> mergePreAggWindow(AggregateWindowState preAggWindow, int streamId) {
        List<DistributedAggregateWindowState> finalPreAggregateWindows = new ArrayList<>();

        WindowAggregateId windowId = preAggWindow.getWindowAggregateId();
        List<AggregateFunction> aggregateFunctions = preAggWindow.getAggregateFunctions();

        final List aggValues = preAggWindow.getAggValues();
        for (int functionId = 0; functionId < aggValues.size(); functionId++) {
            final AggregateFunction aggregateFunction = aggregateFunctions.get(functionId);
            FunctionWindowAggregateId functionWindowId =
                    new FunctionWindowAggregateId(windowId, functionId, this.childId, streamId);

            final Optional<FunctionWindowAggregateId> triggerId;
            final WindowMerger currentMerger;

            if (aggregateFunction instanceof DistributiveAggregateFunction) {
                Integer partialAggregate = (Integer) aggValues.get(functionId);
                triggerId = this.distributiveWindowMerger.processPreAggregate(partialAggregate, functionWindowId);
                currentMerger = this.distributiveWindowMerger;
            } else if (aggregateFunction instanceof AlgebraicAggregateFunction) {
                AlgebraicPartial partial = (AlgebraicPartial) aggValues.get(functionId);
                triggerId = this.algebraicWindowMerger.processPreAggregate(partial, functionWindowId);
                currentMerger = this.algebraicWindowMerger;
            } else if (aggregateFunction instanceof HolisticAggregateHelper) {
                List<Slice> slices = preAggWindow.getSlices();
                triggerId = this.localHolisticWindowMerger.processPreAggregate(slices, functionWindowId);
                currentMerger = this.localHolisticWindowMerger;
            } else {
                throw new RuntimeException("Unsupported aggregate function: " + aggregateFunction);
            }

            if (triggerId.isPresent()) {
                DistributedAggregateWindowState finalPreAggregateWindow = currentMerger.triggerFinalWindow(triggerId.get());
                finalPreAggregateWindows.add(finalPreAggregateWindow);
            }
        }

        return finalPreAggregateWindows;
    }
}
