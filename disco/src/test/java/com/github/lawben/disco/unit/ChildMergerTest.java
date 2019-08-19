package com.github.lawben.disco.unit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.merge.ChildMerger;
import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.BaseWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticAggregateHelper;
import com.github.lawben.disco.aggregation.HolisticWindowAggregate;
import com.github.lawben.disco.utils.ExpectedWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ChildMergerTest {

    void assertWindowValuesEqual(DistributedAggregateWindowState window) {
        assertWindowValuesEqual(window, new ArrayList<>());
    }

    void assertWindowValuesEqual(DistributedAggregateWindowState window, Integer... expectedValues) {
        List<Long> longValues = Arrays.stream(expectedValues).map(Long::valueOf).collect(Collectors.toList());
        assertWindowValuesEqual(window, longValues);
    }

    void assertWindowValuesEqual(DistributedAggregateWindowState window, List<Long> expectedValues) {
        List<Long> aggValues = window.getAggValues();
        assertThat(aggValues, equalTo(expectedValues));
    }

    void assertWindowEquals(ExpectedWindow expected, DistributedAggregateWindowState actual) {
        assertThat(expected.getFunctionWindowId(), equalTo(actual.getFunctionWindowId()));

        List<BaseWindowAggregate> expectedWindowAggregates = expected.getExpectedWindowAggregates();
        assertThat(expectedWindowAggregates, hasSize(1));

        HolisticWindowAggregate expectedWindowAggregate = (HolisticWindowAggregate) expectedWindowAggregates.get(0);
        List<DistributedSlice> expectedSlices = expectedWindowAggregate.getValue();
        List<Slice> actualSlices = (List<Slice>) actual.getAggValues().get(0);
        assertThat(actualSlices, hasSize(expectedSlices.size()));

        for (int i = 0; i < expectedSlices.size(); i++) {
            Slice actualSlice = actualSlices.get(i);
            DistributedSlice expectedSlice = expectedSlices.get(i);
            assertThat(actualSlice.getTStart(), equalTo(expectedSlice.getTStart()));
            assertThat(actualSlice.getTLast(), equalTo(expectedSlice.getTLast()));
            assertTrue(actualSlice.getAggState().hasValues());
            List<Long> aggValues = (List<Long>) actualSlice.getAggState().getValues().get(0);
            assertThat(aggValues, equalTo(expectedSlice.getValues()));
        }
    }

    @Test
    void testAddNewKey() {
        List<Window> windows = Arrays.asList(new TumblingWindow(WindowMeasure.Time, 10, 0));
        List<AggregateFunction> aggFns = Arrays.asList(DistributedUtils.aggregateFunctionSum());

        ChildMerger merger = new ChildMerger(windows, aggFns, 0);

        merger.processElement( 1,  0L, 0);
        merger.processElement(10,  1L, 0);

        List<DistributedAggregateWindowState> result1 = merger.processWatermarkedWindows(10);
        assertThat(result1, hasSize(1));
        assertWindowValuesEqual(result1.get(0), 11);

        merger.processElement( 4, 15L, 0);
        merger.processElement( 5, 17L, 0);
        merger.processElement(11, 19L, 0);

        List<DistributedAggregateWindowState> result2 = merger.processWatermarkedWindows(20);
        assertThat(result2, hasSize(1));
        assertWindowValuesEqual(result2.get(0), 20);
    }

    @Test
    void testAddNewKeys() {
        List<Window> windows = Arrays.asList(new TumblingWindow(WindowMeasure.Time, 10, 0));
        List<AggregateFunction> aggFns = Arrays.asList(DistributedUtils.aggregateFunctionSum());

        ChildMerger merger = new ChildMerger(windows, aggFns, 0);

        merger.processElement( 1,  0L, 0);
        merger.processElement(10,  1L, 1);

        List<DistributedAggregateWindowState> result1 = merger.processWatermarkedWindows(10);
        assertThat(result1, hasSize(2));
        assertWindowValuesEqual(result1.get(0),  1);
        assertWindowValuesEqual(result1.get(1), 10);

        merger.processElement( 4, 15L, 0);
        merger.processElement( 5, 17L, 2);
        merger.processElement(11, 19L, 1);

        List<DistributedAggregateWindowState> result2 = merger.processWatermarkedWindows(20);
        assertThat(result2, hasSize(3));
        assertWindowValuesEqual(result2.get(0),  4);
        assertWindowValuesEqual(result2.get(1), 11);
        assertWindowValuesEqual(result2.get(2),  5);
    }

    @Test
    void testAddNewKeySliding() {
        List<Window> windows = Arrays.asList(new SlidingWindow(WindowMeasure.Time, 10, 5, 0));
        List<AggregateFunction> aggFns = Arrays.asList(DistributedUtils.aggregateFunctionSum());

        ChildMerger merger = new ChildMerger(windows, aggFns, 0);

        merger.processElement( 1,  0L, 0);
        merger.processElement(10,  1L, 0);

        List<DistributedAggregateWindowState> result1 = merger.processWatermarkedWindows(10);
        assertThat(result1, hasSize(1));
        assertWindowValuesEqual(result1.get(0), 11);

        merger.processElement( 4, 15L, 0);
        merger.processElement( 5, 17L, 0);
        merger.processElement(11, 19L, 0);

        List<DistributedAggregateWindowState> result2 = merger.processWatermarkedWindows(20);
        assertThat(result2, hasSize(2));
        assertWindowValuesEqual(result2.get(0));
        assertWindowValuesEqual(result2.get(1), 20);
    }

    @Test
    void testAddNewKeysSliding() {
        List<Window> windows = Arrays.asList(new SlidingWindow(WindowMeasure.Time, 10, 5, 0));
        List<AggregateFunction> aggFns = Arrays.asList(DistributedUtils.aggregateFunctionSum());

        ChildMerger merger = new ChildMerger(windows, aggFns, 0);

        merger.processElement( 1,  0L, 0);
        merger.processElement(10,  1L, 1);

        List<DistributedAggregateWindowState> result1 = merger.processWatermarkedWindows(10);
        assertThat(result1, hasSize(2));
        assertWindowValuesEqual(result1.get(0),  1);
        assertWindowValuesEqual(result1.get(1), 10);

        merger.processElement( 4, 15L, 0);
        merger.processElement( 5, 17L, 2);
        merger.processElement(11, 19L, 1);

        List<DistributedAggregateWindowState> result2 = merger.processWatermarkedWindows(20);
        assertThat(result2, hasSize(6));
        assertWindowValuesEqual(result2.get(0));
        assertWindowValuesEqual(result2.get(1),  4);
        assertWindowValuesEqual(result2.get(2));
        assertWindowValuesEqual(result2.get(3), 11);
        assertWindowValuesEqual(result2.get(4));
        assertWindowValuesEqual(result2.get(5),  5);
    }

    @Test
    void testTwoStreamsSessionMedianTwoKeys() {
        List<Window> windows = Arrays.asList(new SessionWindow(WindowMeasure.Time, 100, 0));
        List<AggregateFunction> aggFns = Arrays.asList(new HolisticAggregateHelper());

        int childId = 0;
        int key1 = 0;
        int key2 = 1;

        ChildMerger merger = new ChildMerger(windows, aggFns, childId);
        merger.processElement(  1,   0, key1);
        merger.processElement(  1,   0, key2);
        merger.processElement(  2,   1, key1);
        merger.processElement(  3,   4, key1);
        merger.processElement(  4,   5, key1);
        merger.processElement(  5,   6, key1);
        merger.processElement(  6,  10, key1);
        merger.processElement(  2,  15, key2);
        merger.processElement(  3,  30, key2);
        merger.processElement(  0, 120, key1);
        merger.processElement(  5, 140, key1);
        merger.processElement( 10, 170, key1);
        merger.processElement(  0, 400, key1);
        List<DistributedAggregateWindowState> result1 = merger.processWatermarkedWindows(100);
        merger.processElement(  5, 405, key1);
        List<DistributedAggregateWindowState> result2 = merger.processWatermarkedWindows(200);
        merger.processElement( 15, 410, key1);
        List<DistributedAggregateWindowState> result3 = merger.processWatermarkedWindows(300);
        merger.processElement( 15, 460, key2);
        merger.processElement(  5, 500, key2);
        List<DistributedAggregateWindowState> result4 = merger.processWatermarkedWindows(400);
        merger.processElement(100, 550, key1);
        merger.processElement(  0, 560, key1);
        merger.processElement( 20, 590, key2);
        merger.processElement(100, 700, key2);
        List<DistributedAggregateWindowState> result5 = merger.processWatermarkedWindows(500);
        merger.processElement(  0, 750, key2);
        List<DistributedAggregateWindowState> result6 = merger.processWatermarkedWindows(600);
        List<DistributedAggregateWindowState> result7 = merger.processWatermarkedWindows(700);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  10, Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L)),
                new DistributedSlice(120, 170, Arrays.asList(0L, 5L, 10L)),
                new DistributedSlice(400, 410, Arrays.asList(0L, 5L, 15L)),
                new DistributedSlice(550, 560, Arrays.asList(100L, 0L))
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  30, Arrays.asList(1L, 2L, 3L)),
                new DistributedSlice(460, 590, Arrays.asList(15L, 5L, 20L)),
                new DistributedSlice(700, 750, Arrays.asList(100L, 0L))
        );

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 110), 0, childId, key1), new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(0)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 130), 0, childId, key2), new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(0)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 120, 270), 0, childId, key1), new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(1)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 510), 0, childId, key1), new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(2)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 550, 660), 0, childId, key1), new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(3)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 460, 690), 0, childId, key2), new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(1))))
        );

        assertThat(result1, empty());
        assertThat(result4, empty());
        assertThat(result5, empty());

        assertThat(result2, hasSize(2));
        DistributedAggregateWindowState window2a = result2.get(0);
        assertWindowEquals(expectedWindows.get(0), window2a);
        DistributedAggregateWindowState window2b = result2.get(1);
        assertWindowEquals(expectedWindows.get(1), window2b);

        assertThat(result3, hasSize(1));
        DistributedAggregateWindowState window3 = result3.get(0);
        assertWindowEquals(expectedWindows.get(2), window3);

        assertThat(result6, hasSize(1));
        DistributedAggregateWindowState window6 = result6.get(0);
        assertWindowEquals(expectedWindows.get(3), window6);

        assertThat(result7, hasSize(2));
        DistributedAggregateWindowState window7a = result7.get(0);
        assertWindowEquals(expectedWindows.get(4), window7a);
        DistributedAggregateWindowState window7b = result7.get(1);
        assertWindowEquals(expectedWindows.get(5), window7b);
    }
}
