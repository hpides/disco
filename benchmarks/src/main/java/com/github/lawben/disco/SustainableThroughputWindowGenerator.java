package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.HOLISTIC_STRING;
import static com.github.lawben.disco.DistributedUtils.aggregateFunctionMax;
import static com.github.lawben.disco.DistributedUtils.maxAggregateFunctionAverage;
import static com.github.lawben.disco.DistributedUtils.maxAggregateFunctionMedian;

import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.AlgebraicWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.DistributiveWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.aggregation.functions.PartialAverage;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class SustainableThroughputWindowGenerator extends SustainableThroughputGenerator {

    public static Function<Long, List<String>> getWindowGen(int childId, String aggFn) {
        WindowGenerator windowGenerator = new WindowGenerator(childId, aggFn);
        return (realTime) -> windowGenerator.getNextWindow();
    }

    public SustainableThroughputWindowGenerator(int childId, int numWindowsPerSecond, String aggFn) {
        super(numWindowsPerSecond, getWindowGen(childId, aggFn));
    }

    private static class WindowGenerator {
        int childId;
        int currentWindowNum;
        String aggFn;

        final long windowSize = 1000;

        MemoryStateFactory factory;
        final List<AggregateFunction> functions;
        final List<Long> sliceValues;
        String sliceString;

        public WindowGenerator(int childId, String aggFn) {
            this.childId = childId;
            this.currentWindowNum = 0;
            this.aggFn = aggFn;

            this.sliceValues = new ArrayList<>(25_000);
            this.factory = new MemoryStateFactory();

            switch (aggFn) {
                case "MAX": {
                    this.functions = List.of(aggregateFunctionMax());
                    break;
                }
                case "M_AVG": {
                    this.functions = List.of(new AlgebraicMergeFunction(maxAggregateFunctionAverage()));
                    break;
                }
                case "M_MEDIAN": {
                    this.functions = List.of(new HolisticMergeWrapper(maxAggregateFunctionMedian()));
                    final long currentTime = System.currentTimeMillis();
                    for (int i = 0; i < 25_000; i++) {
                        sliceValues.add(currentTime);
                    }
                    List<String> sliceStrings = this.sliceValues.stream().map(Object::toString).collect(Collectors.toList());
                    sliceString = String.join(",", sliceStrings);
                    break;
                }
                default:
                    throw new RuntimeException("Unknown aggFn: " + aggFn);
            }

        }

        public List<String> getNextWindow() {
            final long windowStart = windowSize * currentWindowNum;
            final long windowEnd = windowStart + windowSize;
            WindowAggregateId windowAggId = new WindowAggregateId(0, windowStart, windowEnd);
            FunctionWindowAggregateId windowId = new FunctionWindowAggregateId(windowAggId, 0, childId);

            final DistributedAggregateWindowState window;
            switch (aggFn) {
                case "MAX": {
                    AggregateState<Long> state = new AggregateState<>(factory, functions);
                    state.addElement(100000000L);
                    window = new DistributedAggregateWindowState<>(windowId, state);
                    break;
                }
                case "M_AVG": {
                    AggregateState<PartialAverage> state = new AggregateState<>(factory, functions);
                    state.addElement(new PartialAverage(1000000000L, 1));
                    window = new DistributedAggregateWindowState<>(windowId, state);
                    break;
                }
                case "M_MEDIAN": {
                    AggregateState<List<DistributedSlice>> state = new AggregateState<>(factory, functions);
                    state.addElement(List.of(new DistributedSlice(windowStart, windowEnd, sliceValues)));
                    window = new DistributedAggregateWindowState<>(windowId, state);
                    break;
                }
                default:
                    throw new RuntimeException("Unknown aggFn: " + aggFn);
            }

            currentWindowNum++;
            return serializedFunctionWindows(windowId, List.of(window), childId);
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
                List<DistributedSlice> slices = (List<DistributedSlice>) aggValues.get(0);
                // Add functionId as slice might have multiple states
                String slicesString = this.slicesToString(slices.get(0));
                return HOLISTIC_STRING + ":" + slicesString + ":" + key;
            } else {
                throw new IllegalArgumentException("Unknown aggregate function type: " + aggFn.getClass().getSimpleName());
            }
        }


        public String slicesToString(DistributedSlice slice) {
            return String.valueOf(slice.getTStart())
                    + ','
                    + slice.getTLast()
                    + ';'
                    + this.sliceString;
        }
    }
}

