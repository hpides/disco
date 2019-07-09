package com.github.lawben.disco.unit;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.GlobalHolisticWindowMerger;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticNoopFunction;
import com.github.lawben.disco.utils.WindowMergerTestBase;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GlobalHolisticWindowMergerTest extends WindowMergerTestBase {
    @Test
    public void testFinalOneChildMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticNoopFunction(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(1, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        DistributedSlice slice1 = new DistributedSlice(0, 1000, Arrays.asList(1));
        List<DistributedSlice> slices1 = Collections.singletonList(slice1);
        windowMerger.processPreAggregate(slices1, windowId1);
        AggregateWindow<List<DistributedSlice>> final1 = windowMerger.triggerFinalWindow(windowId1);
        assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().size());
        assertThat(final1.getAggValues().get(0), hasItems(slice1));
        Integer finalAgg1 = windowMerger.lowerFinalValue(final1);
        assertThat(finalAgg1, equalTo(1));

        DistributedSlice slice2 = new DistributedSlice(1000, 2000, Arrays.asList(1, 2, 3));
        List<DistributedSlice> slices2 = Collections.singletonList(slice2);
        windowMerger.processPreAggregate(slices2, windowId2);
        AggregateWindow<List<DistributedSlice>> final2 = windowMerger.triggerFinalWindow(windowId2);
        assertTrue(final2.hasValue());
        Assertions.assertEquals(1, final2.getAggValues().size());
        assertThat(final2.getAggValues().get(0), hasItems(slice2));
        Integer finalAgg2 = windowMerger.lowerFinalValue(final2);
        assertThat(finalAgg2, equalTo(2));

        DistributedSlice slice3 = new DistributedSlice(2000, 3000, Arrays.asList(1, 3, 5, 7, 9, 11));
        List<DistributedSlice> slices3 = Collections.singletonList(slice3);
        windowMerger.processPreAggregate(slices3, windowId3);
        AggregateWindow<List<DistributedSlice>> final3 = windowMerger.triggerFinalWindow(windowId3);
        assertTrue(final3.hasValue());
        Assertions.assertEquals(1, final3.getAggValues().size());
        assertThat(final3.getAggValues().get(0), hasItems(slice3));
        Integer finalAgg3 = windowMerger.lowerFinalValue(final3);
        assertThat(finalAgg3, equalTo(7));
    }

    @Test
    public void testFinalTwoChildrenMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticNoopFunction(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(2, windows, aggregateFunctions);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        DistributedSlice slice1a = new DistributedSlice(0, 1000, Arrays.asList(1));
        List<DistributedSlice> slices1a = Collections.singletonList(slice1a);
        FunctionWindowAggregateId functionWindowAggId1 = new FunctionWindowAggregateId(windowId1, 0, 0);
        windowMerger.processPreAggregate(slices1a, functionWindowAggId1);
        Optional<FunctionWindowAggregateId> trigger1a = windowMerger.checkWindowComplete(functionWindowAggId1, true);
        assertTrue(trigger1a.isEmpty());

        DistributedSlice slice1b = new DistributedSlice(0, 1000, Arrays.asList(2));
        List<DistributedSlice> slices1b = Collections.singletonList(slice1b);
        FunctionWindowAggregateId functionWindowAggId2 = new FunctionWindowAggregateId(windowId1, 0, 1);
        windowMerger.processPreAggregate(slices1b, functionWindowAggId2);
        Optional<FunctionWindowAggregateId> trigger1b = windowMerger.checkWindowComplete(functionWindowAggId2, true);
        assertTrue(trigger1b.isPresent());
        AggregateWindow<List<DistributedSlice>> final1b = windowMerger.triggerFinalWindow(trigger1b.get());
        assertTrue(final1b.hasValue());
        Assertions.assertEquals(1, final1b.getAggValues().size());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b));
        Integer finalAgg1 = windowMerger.lowerFinalValue(final1b);
        assertThat(finalAgg1, equalTo(2));

        DistributedSlice slice2a = new DistributedSlice(1000, 2000, Arrays.asList(2, 3, 4));
        List<DistributedSlice> slices2a = Collections.singletonList(slice2a);
        FunctionWindowAggregateId functionWindowAggId3 = new FunctionWindowAggregateId(windowId2, 0, 0);
        windowMerger.processPreAggregate(slices2a, functionWindowAggId3);
        Optional<FunctionWindowAggregateId> trigger2a = windowMerger.checkWindowComplete(functionWindowAggId3, true);
        assertTrue(trigger2a.isEmpty());

        DistributedSlice slice2b = new DistributedSlice(1000, 2000, Arrays.asList(1, 2, 3));
        List<DistributedSlice> slices2b = Collections.singletonList(slice2b);
        FunctionWindowAggregateId functionWindowAggId4 = new FunctionWindowAggregateId(windowId2, 0, 1);
        windowMerger.processPreAggregate(slices2b, functionWindowAggId4);
        Optional<FunctionWindowAggregateId> trigger2b = windowMerger.checkWindowComplete(functionWindowAggId4, true);
        assertTrue(trigger2b.isPresent());
        AggregateWindow<List<DistributedSlice>> final2b = windowMerger.triggerFinalWindow(trigger2b.get());
        assertTrue(final2b.hasValue());
        Assertions.assertEquals(1, final2b.getAggValues().size());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b));
        Integer finalAgg2 = windowMerger.lowerFinalValue(final2b);
        assertThat(finalAgg2, equalTo(3));

        DistributedSlice slice3a = new DistributedSlice(2000, 3000, Arrays.asList(1, 3, 5, 6, 7, 9, 11));
        List<DistributedSlice> slices3a = Collections.singletonList(slice3a);
        FunctionWindowAggregateId functionWindowAggId5 = new FunctionWindowAggregateId(windowId3, 0, 0);
        windowMerger.processPreAggregate(slices3a, functionWindowAggId5);
        Optional<FunctionWindowAggregateId> trigger3a = windowMerger.checkWindowComplete(functionWindowAggId5, true);
        assertTrue(trigger3a.isEmpty());

        DistributedSlice slice3b = new DistributedSlice(2000, 3000, Arrays.asList(1, 3, 5, 7, 9, 11));
        List<DistributedSlice> slices3b = Collections.singletonList(slice3b);
        FunctionWindowAggregateId functionWindowAggId6 = new FunctionWindowAggregateId(windowId3, 0, 1);
        windowMerger.processPreAggregate(slices3b, functionWindowAggId6);
        Optional<FunctionWindowAggregateId> trigger3b = windowMerger.checkWindowComplete(functionWindowAggId6, true);
        assertTrue(trigger3b.isPresent());
        AggregateWindow<List<DistributedSlice>> final3b = windowMerger.triggerFinalWindow(trigger3b.get());
        assertTrue(final3b.hasValue());
        Assertions.assertEquals(1, final3b.getAggValues().size());
        assertThat(final3b.getAggValues().get(0), hasItems(slice3b));
        Integer finalAgg3 = windowMerger.lowerFinalValue(final3b);
        assertThat(finalAgg3, equalTo(6));
    }

    @Test
    public void testFinalTwoChildrenFourStreamsMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticNoopFunction(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(2, windows, aggregateFunctions);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);

        int child1Id = 0;
        int child2Id = 1;
        int stream1Id = 0;
        int stream2Id = 1;

        // Window 1
        DistributedSlice slice11a = new DistributedSlice(0, 1000, Arrays.asList(1));
        List<DistributedSlice> slices11a = Collections.singletonList(slice11a);
        FunctionWindowAggregateId functionWindowAggId11a = new FunctionWindowAggregateId(windowId1, 0, child1Id, stream1Id);
        windowMerger.processPreAggregate(slices11a, functionWindowAggId11a);
        Optional<FunctionWindowAggregateId> trigger11a = windowMerger.checkWindowComplete(functionWindowAggId11a, false);
        assertTrue(trigger11a.isEmpty());

        DistributedSlice slice12a = new DistributedSlice(0, 1000, Arrays.asList(2, 3));
        List<DistributedSlice> slices12a = Collections.singletonList(slice12a);
        FunctionWindowAggregateId functionWindowAggId12a = new FunctionWindowAggregateId(windowId1, 0, child1Id, stream2Id);
        windowMerger.processPreAggregate(slices12a, functionWindowAggId12a);
        Optional<FunctionWindowAggregateId> trigger12a = windowMerger.checkWindowComplete(functionWindowAggId12a, true);
        assertTrue(trigger12a.isEmpty());

        DistributedSlice slice21a = new DistributedSlice(0, 1000, Arrays.asList(4));
        List<DistributedSlice> slices21a = Collections.singletonList(slice21a);
        FunctionWindowAggregateId functionWindowAggId21a = new FunctionWindowAggregateId(windowId1, 0, child2Id, stream1Id);
        windowMerger.processPreAggregate(slices21a, functionWindowAggId21a);
        Optional<FunctionWindowAggregateId> trigger21a = windowMerger.checkWindowComplete(functionWindowAggId21a, false);
        assertTrue(trigger21a.isEmpty());

        DistributedSlice slice22a = new DistributedSlice(0, 1000, Arrays.asList(0));
        List<DistributedSlice> slices22a = Collections.singletonList(slice22a);
        FunctionWindowAggregateId functionWindowAggId22a = new FunctionWindowAggregateId(windowId1, 0, child2Id, stream2Id);
        windowMerger.processPreAggregate(slices22a, functionWindowAggId22a);
        Optional<FunctionWindowAggregateId> trigger22a = windowMerger.checkWindowComplete(functionWindowAggId22a, true);
        assertTrue(trigger22a.isPresent());
        AggregateWindow<List<DistributedSlice>> final22a = windowMerger.triggerFinalWindow(trigger22a.get());
        assertTrue(final22a.hasValue());
        Assertions.assertEquals(1, final22a.getAggValues().size());
        assertThat(final22a.getAggValues().get(0), hasItems(slice22a));
        Integer finalAgg1 = windowMerger.lowerFinalValue(final22a);
        assertThat(finalAgg1, equalTo(2));

        // Window 2
        DistributedSlice slice11b = new DistributedSlice(1000, 2000, Arrays.asList(2, 3, 4));
        List<DistributedSlice> slices11b = Collections.singletonList(slice11b);
        FunctionWindowAggregateId functionWindowAggId11b = new FunctionWindowAggregateId(windowId2, 0, child1Id, stream1Id);
        windowMerger.processPreAggregate(slices11b, functionWindowAggId11b);
        Optional<FunctionWindowAggregateId> trigger11b = windowMerger.checkWindowComplete(functionWindowAggId11b, false);
        assertTrue(trigger11b.isEmpty());

        DistributedSlice slice12b = new DistributedSlice(1000, 2000, Arrays.asList(5, 6, 7));
        List<DistributedSlice> slices12b = Collections.singletonList(slice12b);
        FunctionWindowAggregateId functionWindowAggId12b = new FunctionWindowAggregateId(windowId2, 0, child1Id, stream2Id);
        windowMerger.processPreAggregate(slices12b, functionWindowAggId12b);
        Optional<FunctionWindowAggregateId> trigger12b = windowMerger.checkWindowComplete(functionWindowAggId12b, true);
        assertTrue(trigger12b.isEmpty());

        DistributedSlice slice21b = new DistributedSlice(1000, 2000, Arrays.asList(4));
        List<DistributedSlice> slices21b = Collections.singletonList(slice21b);
        FunctionWindowAggregateId functionWindowAggId21b = new FunctionWindowAggregateId(windowId2, 0, child2Id, stream1Id);
        windowMerger.processPreAggregate(slices21b, functionWindowAggId21b);
        Optional<FunctionWindowAggregateId> trigger21b = windowMerger.checkWindowComplete(functionWindowAggId21b, false);
        assertTrue(trigger21b.isEmpty());

        DistributedSlice slice22b = new DistributedSlice(1000, 2000, Arrays.asList(1, 2, 3));
        List<DistributedSlice> slices22b = Collections.singletonList(slice22b);
        FunctionWindowAggregateId functionWindowAggId22b = new FunctionWindowAggregateId(windowId2, 0, child2Id, stream2Id);
        windowMerger.processPreAggregate(slices22b, functionWindowAggId22b);
        Optional<FunctionWindowAggregateId> trigger22b = windowMerger.checkWindowComplete(functionWindowAggId22b, true);
        assertTrue(trigger22b.isPresent());
        AggregateWindow<List<DistributedSlice>> final22b = windowMerger.triggerFinalWindow(trigger22b.get());
        assertTrue(final22b.hasValue());
        Assertions.assertEquals(1, final22b.getAggValues().size());
        assertThat(final22b.getAggValues().get(0), hasItems(slice22b));
        Integer finalAgg2 = windowMerger.lowerFinalValue(final22b);
        assertThat(finalAgg2, equalTo(4));
    }

    @Test
    public void testFinalOneChildTwoWindowsMedian() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(new HolisticNoopFunction(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(5, windows, aggregateFunctions);

        WindowAggregateId windowId1a = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId1b = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId1c = new WindowAggregateId(1, 2000, 3000);

        WindowAggregateId windowId2a = new WindowAggregateId(2,    0, 1000);
        WindowAggregateId windowId2b = new WindowAggregateId(2,  500, 1500);
        WindowAggregateId windowId2c = new WindowAggregateId(2, 1000, 2000);
        WindowAggregateId windowId2d = new WindowAggregateId(2, 1500, 2500);
        WindowAggregateId windowId2e = new WindowAggregateId(2, 2000, 3000);

        DistributedSlice slice1a1 = new DistributedSlice(0, 500, Arrays.asList(1));
        DistributedSlice slice1a2 = new DistributedSlice(500, 1000, Arrays.asList(2));
        List<DistributedSlice> slices1a = Arrays.asList(slice1a1, slice1a2);
        FunctionWindowAggregateId functionWindowAggregateId1a = new FunctionWindowAggregateId(windowId1a, 0, 0);
        windowMerger.processPreAggregate(slices1a, functionWindowAggregateId1a);
        AggregateWindow<List<DistributedSlice>> final1a = windowMerger.triggerFinalWindow(functionWindowAggregateId1a);
        assertTrue(final1a.hasValue());
        assertThat(final1a.getAggValues().get(0), hasItems(slice1a1, slice1a2));
        Integer finalAgg1a = windowMerger.lowerFinalValue(final1a);
        assertThat(finalAgg1a, equalTo(2));

        DistributedSlice slice2a1 = slice1a1;
        DistributedSlice slice2a2 = slice1a2;
        List<DistributedSlice> slices2a = Collections.emptyList();
        FunctionWindowAggregateId functionWindowAggregateId2a = new FunctionWindowAggregateId(windowId2a, 0, 0);
        windowMerger.processPreAggregate(slices2a, functionWindowAggregateId2a);
        AggregateWindow<List<DistributedSlice>> final2a = windowMerger.triggerFinalWindow(functionWindowAggregateId2a);
        assertTrue(final2a.hasValue());
        assertThat(final2a.getAggValues().get(0), hasItems(slice2a1, slice2a2));
        Integer finalAgg2a = windowMerger.lowerFinalValue(final2a);
        assertThat(finalAgg2a, equalTo(2));

        DistributedSlice slice2b1 = slice2a2;
        DistributedSlice slice2b2 = new DistributedSlice(1000, 1500, Arrays.asList(3, 4));
        List<DistributedSlice> slices2b = Arrays.asList(slice2b2);
        FunctionWindowAggregateId functionWindowAggregateId2b = new FunctionWindowAggregateId(windowId2b, 0, 0);
        windowMerger.processPreAggregate(slices2b, functionWindowAggregateId2b);
        AggregateWindow<List<DistributedSlice>> final2b = windowMerger.triggerFinalWindow(functionWindowAggregateId2b);
        assertTrue(final2b.hasValue());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b1, slice2b2));
        Integer finalAgg2b = windowMerger.lowerFinalValue(final2b);
        assertThat(finalAgg2b, equalTo(3));

        DistributedSlice slice1b1 = slice2b2;
        DistributedSlice slice1b2 = new DistributedSlice(1500, 2000, Arrays.asList(5, 6, 7));
        List<DistributedSlice> slices1b = Arrays.asList(slice1b2);
        FunctionWindowAggregateId functionWindowAggregateId1b = new FunctionWindowAggregateId(windowId1b, 0, 0);
        windowMerger.processPreAggregate(slices1b, functionWindowAggregateId1b);
        AggregateWindow<List<DistributedSlice>> final1b = windowMerger.triggerFinalWindow(functionWindowAggregateId1b);
        assertTrue(final1b.hasValue());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b1, slice1b2));
        Integer finalAgg1b = windowMerger.lowerFinalValue(final1b);
        assertThat(finalAgg1b, equalTo(5));
        
        DistributedSlice slice2c1 = slice1b1;
        DistributedSlice slice2c2 = slice1b2;
        List<DistributedSlice> slices2c = Collections.emptyList();
        FunctionWindowAggregateId functionWindowAggregateId2c = new FunctionWindowAggregateId(windowId2c, 0, 0);
        windowMerger.processPreAggregate(slices2c, functionWindowAggregateId2c);
        AggregateWindow<List<DistributedSlice>> final2c = windowMerger.triggerFinalWindow(functionWindowAggregateId2c);
        assertTrue(final2c.hasValue());
        assertThat(final2c.getAggValues().get(0), hasItems(slice2c1, slice2c2));
        Integer finalAgg2c = windowMerger.lowerFinalValue(final2c);
        assertThat(finalAgg2c, equalTo(5));

        DistributedSlice slice2d1 = slice1b2;
        DistributedSlice slice2d2 = new DistributedSlice(2000, 2500, Arrays.asList(7, 3));
        List<DistributedSlice> slices2d = Arrays.asList(slice2d2);
        FunctionWindowAggregateId functionWindowAggregateId2d = new FunctionWindowAggregateId(windowId2d, 0, 0);
        windowMerger.processPreAggregate(slices2d, functionWindowAggregateId2d);
        AggregateWindow<List<DistributedSlice>> final2d = windowMerger.triggerFinalWindow(functionWindowAggregateId2d);
        assertTrue(final2d.hasValue());
        assertThat(final2d.getAggValues().get(0), hasItems(slice2d1, slice2d2));
        Integer finalAgg2d = windowMerger.lowerFinalValue(final2d);
        assertThat(finalAgg2d, equalTo(6));

        DistributedSlice slice1c1 = slice2d2;
        DistributedSlice slice1c2 = new DistributedSlice(2500, 3000, Arrays.asList(1, 4));
        List<DistributedSlice> slices1c = Arrays.asList(slice1c2);
        FunctionWindowAggregateId functionWindowAggregateId1c = new FunctionWindowAggregateId(windowId1c, 0, 0);
        windowMerger.processPreAggregate(slices1c, functionWindowAggregateId1c);
        AggregateWindow<List<DistributedSlice>> final1c = windowMerger.triggerFinalWindow(functionWindowAggregateId1c);
        assertTrue(final1c.hasValue());
        assertThat(final1c.getAggValues().get(0), hasItems(slice1c1, slice1c2));
        Integer finalAgg1c = windowMerger.lowerFinalValue(final1c);
        assertThat(finalAgg1c, equalTo(4));

        DistributedSlice slice2e1 = slice1c1;
        DistributedSlice slice2e2 = slice1c2;
        List<DistributedSlice> slices2e = Arrays.asList();
        FunctionWindowAggregateId functionWindowAggregateId2e = new FunctionWindowAggregateId(windowId2e, 0, 0);
        windowMerger.processPreAggregate(slices2e, functionWindowAggregateId2e);
        AggregateWindow<List<DistributedSlice>> final2e = windowMerger.triggerFinalWindow(functionWindowAggregateId2e);
        assertTrue(final2e.hasValue());
        assertThat(final2e.getAggValues().get(0), hasItems(slice2e1, slice2e2));
        Integer finalAgg2e = windowMerger.lowerFinalValue(final2e);
        assertThat(finalAgg2e, equalTo(4));
    }
}
