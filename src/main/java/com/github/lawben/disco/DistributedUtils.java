package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.AverageAggregateFunction;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.aggregation.MedianAggregateFunction;
import com.github.lawben.disco.aggregation.SumAggregationFunction;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class DistributedUtils {

    public final static String STREAM_END = "STREAM_END";
    public final static int DEFAULT_SOCKET_TIMEOUT_MS = 500;

    public static final String DISTRIBUTIVE_STRING = "DIS";
    public static final String ALGEBRAIC_STRING = "ALG";
    public static final String HOLISTIC_STRING = "HOL";

    public static final String WINDOW_COMPLETE = "C";
    public static final String WINDOW_PARTIAL = "P";

    public static final String EVENT_STRING = "E";

    public static byte[] objectToBytes(Object object) {
        if (object instanceof Integer) {
            return integerToByte((Integer) object);
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[]{0};
    }

    public static byte[] integerToByte(int i) {
        return new byte[]{
            (byte)(i >>> 24),
            (byte)(i >>> 16),
            (byte)(i >>> 8),
            (byte)i
        };
    }

    public static Object bytesToObject(byte[] bytes) {
        if (bytes.length == 4) {
            // Is integer
            int value = bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
            return value;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            ObjectInputStream in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (Exception e) {
            // The object is null, cannot convert it.
            e.printStackTrace();
            return null;
        }
    }

    public static String buildTcpUrl(String ip, int port) {
        return "tcp://" + ip + ":" + port;
    }

    public static String buildBindingTcpUrl(int port) {
        return buildTcpUrl("*", port);
    }

    public static String buildIpcUrl(String path) {
        return "ipc://" + path;
    }

    public static WindowMeasure windowMeasureFromString(String measureString) {
        if (measureString.equals("COUNT")) {
            return WindowMeasure.Count;
        } else if (measureString.equals("TIME")) {
            return WindowMeasure.Time;
        }

        throw new IllegalArgumentException("Unknown measure: " + measureString);
    }

    public static Window buildWindowFromString(String windowString) {
        String[] windowDetails = windowString.split(",");
        assert windowDetails.length > 0;
        switch (windowDetails[0]) {
            case "TUMBLING": {
                assert windowDetails.length >= 2;
                final long size = Integer.parseInt(windowDetails[1]);
                final long windowId = windowDetails.length >= 3 ? Integer.parseInt(windowDetails[2]) : -1;
                final WindowMeasure measure = windowDetails.length >= 4
                        ? windowMeasureFromString(windowDetails[3])
                        : WindowMeasure.Time;
                return new TumblingWindow(measure, size, windowId);
            }
            case "SLIDING": {
                assert windowDetails.length >= 3;
                final long size = Integer.parseInt(windowDetails[1]);
                final long slide = Integer.parseInt(windowDetails[2]);
                final long windowId = windowDetails.length == 4 ? Integer.parseInt(windowDetails[3]) : -1;
                final WindowMeasure measure = windowDetails.length >= 5
                        ? windowMeasureFromString(windowDetails[4])
                        : WindowMeasure.Time;
                return new SlidingWindow(measure, size, slide, windowId);
            }
            case "SESSION": {
                assert windowDetails.length >= 2;
                final long gap = Integer.parseInt(windowDetails[1]);
                final long windowId = windowDetails.length == 3 ? Integer.parseInt(windowDetails[2]) : -1;
                return new SessionWindow(WindowMeasure.Time, gap, windowId);
            }
            default: {
                throw new IllegalArgumentException("No window type known for: '" + windowDetails[0] + "'");
            }
        }
    }

    public static AggregateFunction buildAggregateFunctionFromString(String aggFnString) {
        switch (aggFnString) {
            case "SUM": {
                return aggregateFunctionSum();
            }
            case "AVG": {
                return aggregateFunctionAverage();
            }
            case "MEDIAN": {
                return aggregateFunctionMedian();
            }
            default: {
                throw new IllegalArgumentException("No aggFn known for: '" + aggFnString + "'");
            }
        }
    }


    public static String functionWindowIdToString(FunctionWindowAggregateId functionWindowAggId) {
        WindowAggregateId windowId = functionWindowAggId.getWindowId();

        List<Number> values = new ArrayList<>();
        values.add(windowId.getWindowId());
        values.add(windowId.getWindowStartTimestamp());
        values.add(windowId.getWindowEndTimestamp());
        values.add(functionWindowAggId.getFunctionId());
        values.add(functionWindowAggId.getChildId());
        values.add(functionWindowAggId.getKey());

        List<String> stringValues = values.stream().map(String::valueOf).collect(Collectors.toList());
        return String.join(",", stringValues);
    }

    public static FunctionWindowAggregateId stringToFunctionWindowAggId(String rawString) {
        List<Long> windowIdSplit = stringToLongs(rawString);
        assert windowIdSplit.size() == 6;
        WindowAggregateId windowId = new WindowAggregateId(windowIdSplit.get(0), windowIdSplit.get(1), windowIdSplit.get(2));
        int functionId = Math.toIntExact(windowIdSplit.get(3));
        int childId = Math.toIntExact(windowIdSplit.get(4));
        int key = Math.toIntExact(windowIdSplit.get(5));
        return new FunctionWindowAggregateId(windowId, functionId, childId, key);
    }

    public static List<Long> stringToLongs(String rawString) {
        String[] strings = rawString.split(",");
        List<Long> longs = new ArrayList<>(strings.length);
        for (String string : strings) {
            longs.add(Long.valueOf(string));
        }
        return longs;
    }

    public static String slicesToString(List<? extends Slice> slices, int functionId) {
        List<String> allSlices = new ArrayList<>(slices.size());

        for (Slice slice : slices) {
            List<List<Integer>> aggValues = slice.getAggState().getValues();
            if (aggValues.isEmpty()) {
                continue;
            }

            StringBuilder sb = new StringBuilder();
            sb.append(slice.getTStart());
            sb.append(',');
            sb.append(slice.getTLast());
            sb.append(';');

            List<Integer> values = aggValues.get(functionId);
            List<String> valueStrings = values.stream().map(String::valueOf).collect(Collectors.toList());
            sb.append(String.join(",", valueStrings));
            allSlices.add(sb.toString());
        }

        return String.join("/", allSlices);
    }

    public static String slicesToString(List<? extends Slice> slices) {
        return slicesToString(slices, 0);
    }


    public static List<DistributedSlice> slicesFromString(String slicesString) {
        List<DistributedSlice> slices = new ArrayList<>();
        if (slicesString.isEmpty()) {
            return slices;
        }

        for (String sliceString : slicesString.split("/")) {
            slices.add(singleSliceFromString(sliceString));
        }
        return slices;
    }

    public static DistributedSlice singleSliceFromString(String sliceString) {
        String[] parts = sliceString.split(";");
        String[] times = parts[0].split(",");
        if (times.length != 2) {
            throw new IllegalArgumentException("Slice needs to have 'start,end'. Got: " + parts[0]);
        }

        long start = Long.valueOf(times[0]);
        long end = Long.valueOf(times[1]);

        if (parts.length == 1) {
            return new DistributedSlice(start, end, new ArrayList<>());
        }

        String[] rawValues = parts[1].split(",");
        List<String> valueStrings = Arrays.asList(rawValues);
        List<Integer> values = valueStrings.stream().map(Integer::valueOf).collect(Collectors.toList());

        return new DistributedSlice(start, end, values);
    }

    public static List<Long> getRandomSeeds(String[] args, int numStreams, int position) {
        final List<Long> randomSeeds = new ArrayList<>(numStreams);
        if (args.length >= position + 1) {
            String seedString = args[position];
            String[] seedStringSplit = seedString.split(",");
            assert seedStringSplit.length == numStreams;

            for (String seed : seedStringSplit) {
                randomSeeds.add(Long.valueOf(seed));
            }
        } else {
            Random rand = new Random();
            for (int i = 0; i < numStreams; i++) {
                randomSeeds.add(rand.nextLong());
            }
        }
        return randomSeeds;
    }

    public static AggregateFunction aggregateFunctionSum() {
        return new SumAggregationFunction();
    }

    public static AggregateFunction aggregateFunctionAverage() {
        return new AverageAggregateFunction();
    }

    public static HolisticAggregateFunction aggregateFunctionMedian() {
        return new MedianAggregateFunction();
    }

    public static List<AggregateFunction> convertAggregateFunctions(List<AggregateFunction> aggFns) {
        return aggFns.stream()
                .map(aggFn -> {
                    if (aggFn instanceof AlgebraicAggregateFunction) {
                        return new AlgebraicMergeFunction((AlgebraicAggregateFunction) aggFn);
                    } else if(aggFn instanceof HolisticAggregateFunction) {
                        return new HolisticMergeWrapper((HolisticAggregateFunction) aggFn);
                    } else {
                        return aggFn;
                    }
                })
                .collect(Collectors.toList());
    }
}
