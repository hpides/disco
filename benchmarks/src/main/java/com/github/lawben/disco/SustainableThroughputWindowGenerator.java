package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.HOLISTIC_STRING;

import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.AlgebraicWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.DistributiveWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class SustainableThroughputWindowGenerator extends SustainableThroughputGenerator {

    public static Function<Long, List<String>> getWindowGen(int numChildren) {
        WindowGenerator windowGenerator = new WindowGenerator(numChildren);
        return (realTime) -> windowGenerator.getNextWindow();
    }

    public SustainableThroughputWindowGenerator(int numChildren, int numWindowsPerSecond) {
        super(numWindowsPerSecond, getWindowGen(numChildren));
    }

    private static class WindowGenerator {
        final int numChildren;
        int currentChild;
        int currentWindowNum;

        final long windowSize = 10;

        MemoryStateFactory factory;
        List<AggregateFunction> functions;

        public WindowGenerator(int numChildren) {
            this.numChildren = numChildren;
            this.currentChild = 0;
            this.currentWindowNum = 0;

            this.factory = new MemoryStateFactory();
            this.functions = List.of(DistributedUtils.aggregateFunctionMax());
        }

        public List<String> getNextWindow() {
            final long windowStart = windowSize * currentWindowNum;
            final long windowEnd = windowStart + windowSize;
            WindowAggregateId windowAggId = new WindowAggregateId(0, windowStart, windowEnd);
            FunctionWindowAggregateId windowId = new FunctionWindowAggregateId(windowAggId, 0, currentChild);

            AggregateState<Long> state = new AggregateState<>(factory, functions);
            state.addElement(100L);
            DistributedAggregateWindowState<Long> window = new DistributedAggregateWindowState<>(windowId, state);
            List<String> serializedMsg = serializedFunctionWindows(windowId, List.of(window), currentChild);

            currentChild = (currentChild + 1) % numChildren;
            // Send same window for each child, then increase window
            if (currentChild == 0) currentWindowNum++;

            return serializedMsg;
        }

        private List<String> serializedFunctionWindows(FunctionWindowAggregateId functionWindowAggId,
                List<DistributedAggregateWindowState> aggWindows, int childId) {
            List<String> serializedMessage = new ArrayList<>();
            // Add child id and window id for each aggregate type
            serializedMessage.add(String.valueOf(childId));
            serializedMessage.add(DistributedUtils.functionWindowIdToString(functionWindowAggId));
            serializedMessage.add(String.valueOf(aggWindows.size()));

            for (DistributedAggregateWindowState preAggregatedWindow : aggWindows) {
                String serializedAggWindow = this.serializeAggregate(preAggregatedWindow);
                serializedMessage.add(serializedAggWindow);
            }

            return serializedMessage;
        }

        private String serializeAggregate(DistributedAggregateWindowState aggWindow) {
            List<AggregateFunction> aggFns = aggWindow.getAggregateFunctions();
            if (aggFns.size() > 1) {
                throw new IllegalStateException("Final agg should only have one function.");
            }
            FunctionWindowAggregateId functionWindowAggId = aggWindow.getFunctionWindowId();
            final int key = functionWindowAggId.getKey();

            List aggValues = aggWindow.getAggValues();
            boolean hasValue = !aggValues.isEmpty();
            AggregateFunction aggFn = aggFns.get(0);

            if (aggFn instanceof DistributiveAggregateFunction) {
                Long partialAggregate = hasValue ? (Long) aggValues.get(0) : null;
                return new DistributiveWindowAggregate(partialAggregate, key).asString();
            } else if (aggFn instanceof AlgebraicMergeFunction) {
                AlgebraicPartial partial = hasValue ? (AlgebraicPartial) aggValues.get(0) : null;
                return new AlgebraicWindowAggregate(partial, key).asString();
            } else if (aggFn instanceof HolisticMergeWrapper) {
                List<Slice> slices = hasValue ? (List<Slice>) aggValues.get(0) : new ArrayList<>();
                // Add functionId as slice might have multiple states
                String slicesString = DistributedUtils.slicesToString(slices, functionWindowAggId.getFunctionId());
                return HOLISTIC_STRING + ":" + slicesString + ":" + key;
            } else {
                throw new IllegalArgumentException("Unknown aggregate function type: " + aggFn.getClass().getSimpleName());
            }
        }
    }
}

