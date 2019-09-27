package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicAggregateFunction;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.functions.AverageAggregateFunction;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.DistributiveAggregateFunction;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateFunction;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.aggregation.functions.MaxAggregateFunction;
import com.github.lawben.disco.aggregation.functions.MaxAverageAggregateFunction;
import com.github.lawben.disco.aggregation.functions.MaxMedianAggregateFunction;
import com.github.lawben.disco.aggregation.functions.MaxMinAggregateFunction;
import com.github.lawben.disco.aggregation.functions.MaxSumAggregateFunction;
import com.github.lawben.disco.aggregation.functions.MedianAggregateFunction;
import com.github.lawben.disco.aggregation.functions.MinAggregateFunction;
import com.github.lawben.disco.aggregation.functions.SumAggregateFunction;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.LazySlice;
import de.tub.dima.scotty.slicing.slice.Slice;
import de.tub.dima.scotty.slicing.slice.StreamRecord;
import de.tub.dima.scotty.state.SetState;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DistributedUtils {

    public final static String STREAM_END = "STREAM_END";
    public final static int DEFAULT_SOCKET_TIMEOUT_MS = 500;

    public static final String DISTRIBUTIVE_STRING = "DIS";
    public static final String ALGEBRAIC_STRING = "ALG";
    public static final String HOLISTIC_STRING = "HOL";

    public static final long MAX_LATENESS = 100L;

    public static final String EVENT_STRING = "E";
    public static final String CONTROL_STRING = "C";

    public static final String ARG_DELIMITER = ";";
    public static final String CONCURRENT_WINDOW = "CONCURRENT";


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

    private static Window buildWindowFromString(String windowString) {
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
            case "MAX": {
                return aggregateFunctionMax();
            }
            case "MIN": {
                return aggregateFunctionMin();
            }
            // Special cases for latency measurement
            case "M_SUM": {
                return maxAggregateFunctionSum();
            }
            case "M_AVG": {
                return maxAggregateFunctionAverage();
            }
            case "M_MEDIAN": {
                return maxAggregateFunctionMedian();
            }
            case "M_MIN": {
                return maxAggregateFunctionMin();
            }

            default: {
                throw new IllegalArgumentException("No aggFn known for: '" + aggFnString + "'");
            }
        }
    }


    public static Optional<FunctionWindowAggregateId> getNextSessionStart(
            List<FunctionWindowAggregateId> sessionStarts, long lastSessionEnd) {
        sessionStarts.sort(Comparator.comparingLong(id -> id.getWindowId().getWindowStartTimestamp()));

        Optional<FunctionWindowAggregateId> newSession = Optional.empty();
        int clearIdx = sessionStarts.size();
        for (int sessionStartIdx = 0; sessionStartIdx < sessionStarts.size(); sessionStartIdx++) {
            FunctionWindowAggregateId session = sessionStarts.get(sessionStartIdx);
            if (session.getWindowId().getWindowStartTimestamp() > lastSessionEnd) {
                newSession = Optional.of(session);
                clearIdx = sessionStartIdx + 1;
                break;
            }
        }
        if (newSession.isPresent()) {
            sessionStarts.subList(0, clearIdx).clear();
        }
        return newSession;
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
            List<List<Long>> aggValues = slice.getAggState().getValues();
            if (aggValues.isEmpty()) {
                continue;
            }

            StringBuilder sb = new StringBuilder();
            sb.append(slice.getTStart());
            sb.append(',');
            sb.append(slice.getTLast());
            sb.append(';');

            List<Long> values = aggValues.get(functionId);
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
        if (slicesString == null || slicesString.isEmpty() || slicesString.equals("null")) {
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

        long start = Long.parseLong(times[0]);
        long end = Long.parseLong(times[1]);

        if (parts.length == 1) {
            return new DistributedSlice(start, end, new ArrayList<>());
        }

        String[] rawValues = parts[1].split(",");
        List<String> valueStrings = Arrays.asList(rawValues);
        List<Long> values = valueStrings.stream().map(Long::valueOf).collect(Collectors.toList());

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

    public static DistributiveAggregateFunction aggregateFunctionSum() {
        return new SumAggregateFunction();
    }

    public static AlgebraicAggregateFunction aggregateFunctionAverage() {
        return new AverageAggregateFunction();
    }

    public static HolisticAggregateFunction aggregateFunctionMedian() {
        return new MedianAggregateFunction();
    }

    private static DistributiveAggregateFunction aggregateFunctionMax() {
        return new MaxAggregateFunction();
    }

    private static DistributiveAggregateFunction aggregateFunctionMin() {
        return new MinAggregateFunction();
    }

    public static DistributiveAggregateFunction maxAggregateFunctionSum() {
        return new MaxSumAggregateFunction();
    }

    public static AlgebraicAggregateFunction maxAggregateFunctionAverage() {
        return new MaxAverageAggregateFunction();
    }

    public static HolisticAggregateFunction maxAggregateFunctionMedian() {
        return new MaxMedianAggregateFunction();
    }

    private static DistributiveAggregateFunction maxAggregateFunctionMax() {
        return new MaxAggregateFunction();
    }

    private static DistributiveAggregateFunction maxAggregateFunctionMin() {
        return new MaxMinAggregateFunction();
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

    public static List<Window> createWindowsFromStrings(String[] windowStrings) {
        if (windowStrings.length == 1 && windowStrings[0].startsWith(CONCURRENT_WINDOW)) {
            return buildConcurrentWindowsFromString(windowStrings[0]);
        }

        List<Window> windows = new ArrayList<>(windowStrings.length);
        for (String windowRow : windowStrings) {
            Window window = DistributedUtils.buildWindowFromString(windowRow);
            windows.add(window);
        }

        return windows;
    }

    public static List<Window> createWindowsFromString(String windowString) {
        String[] windowStrings = windowString.split(ARG_DELIMITER);
        return createWindowsFromStrings(windowStrings);
    }

    public static List<Window> buildConcurrentWindowsFromString(String windowString) {
        String[] concurrentWindowParts = windowString.split(",");
        assert concurrentWindowParts.length >= 4;
        assert concurrentWindowParts[0].equals(CONCURRENT_WINDOW);

        final int numConcurrentWindows = Integer.parseInt(concurrentWindowParts[1]);
        final String windowType = concurrentWindowParts[2];
        assert windowType.equals("TUMBLING");
        final int baseWindowLength = Integer.parseInt(concurrentWindowParts[3]);

        List<Window> windows = new ArrayList<>(numConcurrentWindows);
        Random random = new Random();
        final int minWindowLength = baseWindowLength / 2;

        for (int windowId = 1; windowId <= numConcurrentWindows; windowId++) {
            final int windowLength = random.nextInt(baseWindowLength) + minWindowLength;
            String randomWindowString = "TUMBLING," + windowLength + "," + windowId;
            windows.add(buildWindowFromString(randomWindowString));
        }

        return windows;
    }

    public static List<AggregateFunction> createAggFunctionsFromString(String aggFnString) {
        List<AggregateFunction> aggFns = new ArrayList<>();

        String[] aggFnRows = aggFnString.split(ARG_DELIMITER);
        for (String aggFnRow : aggFnRows) {
            AggregateFunction aggFn = DistributedUtils.buildAggregateFunctionFromString(aggFnRow);
            aggFns.add(aggFn);
        }

        return aggFns;
    }

    public static long getWatermarkMsFromWindows(List<Window> windows) {
        List<Window> timedWindows = windows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Time)
                .collect(Collectors.toList());

        List<Long> sessionWatermarkMs = timedWindows.stream()
                .filter(w -> w instanceof SessionWindow)
                .map(w -> ((SessionWindow) w).getGap())
                .collect(Collectors.toList());

        List<Long> tumblingWatermarkMs = timedWindows.stream()
                .filter(w -> w instanceof TumblingWindow)
                .map(w -> ((TumblingWindow) w).getSize())
                .collect(Collectors.toList());

        List<Long> slidingWatermarkMs = timedWindows.stream()
                .filter(w -> w instanceof SlidingWindow)
                .map(w -> ((SlidingWindow) w).getSlide())
                .collect(Collectors.toList());

        return Stream.of(sessionWatermarkMs, slidingWatermarkMs, tumblingWatermarkMs)
                .flatMap(Collection::stream)
                .min(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalArgumentException("Could not find watermark ms."));
    }

    public static long getWatermarkMsFromWindowString(String[] windowStrings) {
        List<Window> windows = createWindowsFromString(String.join(ARG_DELIMITER, windowStrings));
        return getWatermarkMsFromWindows(windows);
    }
}
