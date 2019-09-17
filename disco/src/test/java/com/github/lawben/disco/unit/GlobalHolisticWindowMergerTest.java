package com.github.lawben.disco.unit;

import static com.github.lawben.disco.Event.NO_KEY;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.merge.GlobalHolisticWindowMerger;
import com.github.lawben.disco.utils.WindowMergerTestBase;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class GlobalHolisticWindowMergerTest extends WindowMergerTestBase {
    @Test
    public void testFinalOneChildMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticMergeWrapper(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(1, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        DistributedSlice slice1 = new DistributedSlice(0, 1000, 1);
        List<DistributedSlice> slices1 = Collections.singletonList(slice1);
        windowMerger.processPreAggregate(slices1, windowId1);
        Optional<FunctionWindowAggregateId> trigger1 = windowMerger.checkWindowComplete(windowId1);
        assertTrue(trigger1.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final1All = windowMerger.triggerFinalWindow(trigger1.get());
        assertThat(final1All, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final1 = final1All.get(0); 
        assertThat(final1.getAggValues(), hasSize(1));
        assertThat(final1.getAggValues().get(0), hasItems(slice1));
        Long final1Agg = windowMerger.lowerFinalValue(final1);
        assertThat(final1Agg, equalTo(1L));

        DistributedSlice slice2 = new DistributedSlice(1000, 2000, 1, 2, 3);
        List<DistributedSlice> slices2 = Collections.singletonList(slice2);
        windowMerger.processPreAggregate(slices2, windowId2);
        Optional<FunctionWindowAggregateId> trigger2 = windowMerger.checkWindowComplete(windowId2);
        assertTrue(trigger2.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2All = windowMerger.triggerFinalWindow(trigger2.get());
        assertThat(final2All, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2 = final2All.get(0);
        assertThat(final2.getAggValues(), hasSize(1));
        assertThat(final2.getAggValues().get(0), hasItems(slice2));
        Long final2Agg = windowMerger.lowerFinalValue(final2);
        assertThat(final2Agg, equalTo(2L));

        DistributedSlice slice3 = new DistributedSlice(2000, 3000, 1, 3, 5, 7, 9, 11);
        List<DistributedSlice> slices3 = Collections.singletonList(slice3);
        windowMerger.processPreAggregate(slices3, windowId3);
        Optional<FunctionWindowAggregateId> trigger3 = windowMerger.checkWindowComplete(windowId3);
        assertTrue(trigger3.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final3All = windowMerger.triggerFinalWindow(trigger3.get());
        assertThat(final3All, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final3 = final3All.get(0);
        assertThat(final3.getAggValues(), hasSize(1));
        assertThat(final3.getAggValues().get(0), hasItems(slice3));
        Long final3Agg = windowMerger.lowerFinalValue(final3);
        assertThat(final3Agg, equalTo(7L));
    }

    @Test
    public void testFinalTwoChildrenMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticMergeWrapper(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(2, windows, aggregateFunctions);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        final int childId1 = 0;
        final int childId2 = 1;

        DistributedSlice slice1a = new DistributedSlice(0, 1000, 1);
        List<DistributedSlice> slices1a = Collections.singletonList(slice1a);
        FunctionWindowAggregateId functionWindowAggId1a = new FunctionWindowAggregateId(windowId1, 0, childId1);
        windowMerger.processPreAggregate(slices1a, functionWindowAggId1a);
        Optional<FunctionWindowAggregateId> trigger1a = windowMerger.checkWindowComplete(functionWindowAggId1a);
        assertFalse(trigger1a.isPresent());

        DistributedSlice slice1b = new DistributedSlice(0, 1000, 2);
        List<DistributedSlice> slices1b = Collections.singletonList(slice1b);
        FunctionWindowAggregateId functionWindowAggId1b = new FunctionWindowAggregateId(windowId1, 0, childId2);
        windowMerger.processPreAggregate(slices1b, functionWindowAggId1b);
        Optional<FunctionWindowAggregateId> trigger1b = windowMerger.checkWindowComplete(functionWindowAggId1b);
        assertTrue(trigger1b.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final1bAll = windowMerger.triggerFinalWindow(trigger1b.get());
        assertThat(final1bAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final1b = final1bAll.get(0);
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b));
        Long finalAgg1 = windowMerger.lowerFinalValue(final1b);
        assertThat(finalAgg1, equalTo(2L));

        DistributedSlice slice2a = new DistributedSlice(1000, 2000, 2, 3, 4);
        List<DistributedSlice> slices2a = Collections.singletonList(slice2a);
        FunctionWindowAggregateId functionWindowAggId2a = new FunctionWindowAggregateId(windowId2, 0, 0);
        windowMerger.processPreAggregate(slices2a, functionWindowAggId2a);
        Optional<FunctionWindowAggregateId> trigger2a = windowMerger.checkWindowComplete(functionWindowAggId2a);
        assertFalse(trigger2a.isPresent());

        DistributedSlice slice2b = new DistributedSlice(1000, 2000, 1, 2, 3);
        List<DistributedSlice> slices2b = Collections.singletonList(slice2b);
        FunctionWindowAggregateId functionWindowAggId2b = new FunctionWindowAggregateId(windowId2, 0, 1);
        windowMerger.processPreAggregate(slices2b, functionWindowAggId2b);
        Optional<FunctionWindowAggregateId> trigger2b = windowMerger.checkWindowComplete(functionWindowAggId2b);
        assertTrue(trigger2b.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2bAll = windowMerger.triggerFinalWindow(trigger2b.get());
        assertThat(final2bAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2b = final2bAll.get(0);
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b));
        Long finalAgg2 = windowMerger.lowerFinalValue(final2b);
        assertThat(finalAgg2, equalTo(3L));

        DistributedSlice slice3a = new DistributedSlice(2000, 3000, 1, 3, 5, 6, 7, 9, 11);
        List<DistributedSlice> slices3a = Collections.singletonList(slice3a);
        FunctionWindowAggregateId functionWindowAggId3a = new FunctionWindowAggregateId(windowId3, 0, 0);
        windowMerger.processPreAggregate(slices3a, functionWindowAggId3a);
        Optional<FunctionWindowAggregateId> trigger3a = windowMerger.checkWindowComplete(functionWindowAggId3a);
        assertFalse(trigger3a.isPresent());

        DistributedSlice slice3b = new DistributedSlice(2000, 3000, 1, 3, 5, 7, 9, 11);
        List<DistributedSlice> slices3b = Collections.singletonList(slice3b);
        FunctionWindowAggregateId functionWindowAggId3b = new FunctionWindowAggregateId(windowId3, 0, 1);
        windowMerger.processPreAggregate(slices3b, functionWindowAggId3b);
        Optional<FunctionWindowAggregateId> trigger3b = windowMerger.checkWindowComplete(functionWindowAggId3b);
        assertTrue(trigger3b.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final3bAll = windowMerger.triggerFinalWindow(trigger3b.get());
        assertThat(final3bAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final3b = final3bAll.get(0);
        assertThat(final3b.getAggValues().get(0), hasItems(slice3b));
        Long finalAgg3 = windowMerger.lowerFinalValue(final3b);
        assertThat(finalAgg3, equalTo(6L));
    }

    @Test
    public void testFinalFourChildrenMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticMergeWrapper(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(4, windows, aggregateFunctions);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);

        int child1Id = 0;
        int child2Id = 1;
        int child3Id = 2;
        int child4Id = 3;

        // Window 1
        DistributedSlice slice11a = new DistributedSlice(0, 1000, 1);
        List<DistributedSlice> slices11a = Collections.singletonList(slice11a);
        FunctionWindowAggregateId functionWindowAggId11a = new FunctionWindowAggregateId(windowId1, 0, child1Id);
        windowMerger.processPreAggregate(slices11a, functionWindowAggId11a);
        Optional<FunctionWindowAggregateId> trigger11a = windowMerger.checkWindowComplete(functionWindowAggId11a);
        assertFalse(trigger11a.isPresent());

        DistributedSlice slice12a = new DistributedSlice(0, 1000, 2, 3);
        List<DistributedSlice> slices12a = Collections.singletonList(slice12a);
        FunctionWindowAggregateId functionWindowAggId12a = new FunctionWindowAggregateId(windowId1, 0, child2Id);
        windowMerger.processPreAggregate(slices12a, functionWindowAggId12a);
        Optional<FunctionWindowAggregateId> trigger12a = windowMerger.checkWindowComplete(functionWindowAggId12a);
        assertFalse(trigger12a.isPresent());

        DistributedSlice slice21a = new DistributedSlice(0, 1000, 4);
        List<DistributedSlice> slices21a = Collections.singletonList(slice21a);
        FunctionWindowAggregateId functionWindowAggId21a = new FunctionWindowAggregateId(windowId1, 0, child3Id);
        windowMerger.processPreAggregate(slices21a, functionWindowAggId21a);
        Optional<FunctionWindowAggregateId> trigger21a = windowMerger.checkWindowComplete(functionWindowAggId21a);
        assertFalse(trigger21a.isPresent());

        DistributedSlice slice22a = new DistributedSlice(0, 1000, 0);
        List<DistributedSlice> slices22a = Collections.singletonList(slice22a);
        FunctionWindowAggregateId functionWindowAggId22a = new FunctionWindowAggregateId(windowId1, 0, child4Id);
        windowMerger.processPreAggregate(slices22a, functionWindowAggId22a);
        Optional<FunctionWindowAggregateId> trigger22a = windowMerger.checkWindowComplete(functionWindowAggId22a);
        assertTrue(trigger22a.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final22aAll = windowMerger.triggerFinalWindow(trigger22a.get());
        assertThat(final22aAll, hasSize(1));
        assertThat(final22aAll.get(0).getAggValues().get(0), hasItems(slice22a));
        Long finalAgg1 = windowMerger.lowerFinalValue(final22aAll.get(0));
        assertThat(finalAgg1, equalTo(2L));

        // Window 2
        DistributedSlice slice11b = new DistributedSlice(1000, 2000, 2, 3, 4);
        List<DistributedSlice> slices11b = Collections.singletonList(slice11b);
        FunctionWindowAggregateId functionWindowAggId11b = new FunctionWindowAggregateId(windowId2, 0, child1Id);
        windowMerger.processPreAggregate(slices11b, functionWindowAggId11b);
        Optional<FunctionWindowAggregateId> trigger11b = windowMerger.checkWindowComplete(functionWindowAggId11b);
        assertFalse(trigger11b.isPresent());

        DistributedSlice slice12b = new DistributedSlice(1000, 2000, 5, 6, 7);
        List<DistributedSlice> slices12b = Collections.singletonList(slice12b);
        FunctionWindowAggregateId functionWindowAggId12b = new FunctionWindowAggregateId(windowId2, 0, child2Id);
        windowMerger.processPreAggregate(slices12b, functionWindowAggId12b);
        Optional<FunctionWindowAggregateId> trigger12b = windowMerger.checkWindowComplete(functionWindowAggId12b);
        assertFalse(trigger12b.isPresent());

        DistributedSlice slice21b = new DistributedSlice(1000, 2000, 4);
        List<DistributedSlice> slices21b = Collections.singletonList(slice21b);
        FunctionWindowAggregateId functionWindowAggId21b = new FunctionWindowAggregateId(windowId2, 0, child3Id);
        windowMerger.processPreAggregate(slices21b, functionWindowAggId21b);
        Optional<FunctionWindowAggregateId> trigger21b = windowMerger.checkWindowComplete(functionWindowAggId21b);
        assertFalse(trigger21b.isPresent());

        DistributedSlice slice22b = new DistributedSlice(1000, 2000, 1, 2, 3);
        List<DistributedSlice> slices22b = Collections.singletonList(slice22b);
        FunctionWindowAggregateId functionWindowAggId22b = new FunctionWindowAggregateId(windowId2, 0, child4Id);
        windowMerger.processPreAggregate(slices22b, functionWindowAggId22b);
        Optional<FunctionWindowAggregateId> trigger22b = windowMerger.checkWindowComplete(functionWindowAggId22b);
        assertTrue(trigger22b.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final22bAll = windowMerger.triggerFinalWindow(trigger22b.get());
        assertThat(final22bAll, hasSize(1));
        assertThat(final22bAll.get(0).getAggValues().get(0), hasItems(slice22b));
        Long finalAgg2 = windowMerger.lowerFinalValue(final22bAll.get(0));
        assertThat(finalAgg2, equalTo(4L));
    }

    @Test
    public void testFinalOneChildTwoWindowsMedian() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(new HolisticMergeWrapper(DistributedUtils.aggregateFunctionMedian()));
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(5, windows, aggregateFunctions);

        WindowAggregateId windowId1a = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId1b = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId1c = new WindowAggregateId(1, 2000, 3000);

        WindowAggregateId windowId2a = new WindowAggregateId(2,    0, 1000);
        WindowAggregateId windowId2b = new WindowAggregateId(2,  500, 1500);
        WindowAggregateId windowId2c = new WindowAggregateId(2, 1000, 2000);
        WindowAggregateId windowId2d = new WindowAggregateId(2, 1500, 2500);
        WindowAggregateId windowId2e = new WindowAggregateId(2, 2000, 3000);

        DistributedSlice slice1a1 = new DistributedSlice(0, 500, 1);
        DistributedSlice slice1a2 = new DistributedSlice(500, 1000, 2);
        List<DistributedSlice> slices1a = Arrays.asList(slice1a1, slice1a2);
        FunctionWindowAggregateId functionWindowAggregateId1a = new FunctionWindowAggregateId(windowId1a, 0, 0);
        windowMerger.processPreAggregate(slices1a, functionWindowAggregateId1a);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final1aAll = windowMerger.triggerFinalWindow(functionWindowAggregateId1a);
        assertThat(final1aAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final1a = final1aAll.get(0);
        assertTrue(final1a.hasValue());
        assertThat(final1a.getAggValues().get(0), hasItems(slice1a1, slice1a2));
        Long finalAgg1a = windowMerger.lowerFinalValue(final1a);
        assertThat(finalAgg1a, equalTo(2L));

        DistributedSlice slice2a1 = slice1a1;
        DistributedSlice slice2a2 = slice1a2;
        List<DistributedSlice> slices2a = Collections.emptyList();
        FunctionWindowAggregateId functionWindowAggregateId2a = new FunctionWindowAggregateId(windowId2a, 0, 0);
        windowMerger.processPreAggregate(slices2a, functionWindowAggregateId2a);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2aAll = windowMerger.triggerFinalWindow(functionWindowAggregateId2a);
        assertThat(final2aAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2a = final2aAll.get(0);
        assertTrue(final2a.hasValue());
        assertThat(final2a.getAggValues().get(0), hasItems(slice2a1, slice2a2));
        Long finalAgg2a = windowMerger.lowerFinalValue(final2a);
        assertThat(finalAgg2a, equalTo(2L));

        DistributedSlice slice2b1 = slice2a2;
        DistributedSlice slice2b2 = new DistributedSlice(1000, 1500, 3, 4);
        List<DistributedSlice> slices2b = Arrays.asList(slice2b2);
        FunctionWindowAggregateId functionWindowAggregateId2b = new FunctionWindowAggregateId(windowId2b, 0, 0);
        windowMerger.processPreAggregate(slices2b, functionWindowAggregateId2b);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2bAll = windowMerger.triggerFinalWindow(functionWindowAggregateId2b);
        assertThat(final2bAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2b = final2bAll.get(0);
        assertTrue(final2b.hasValue());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b1, slice2b2));
        Long finalAgg2b = windowMerger.lowerFinalValue(final2b);
        assertThat(finalAgg2b, equalTo(3L));

        DistributedSlice slice1b1 = slice2b2;
        DistributedSlice slice1b2 = new DistributedSlice(1500, 2000, 5, 6, 7);
        List<DistributedSlice> slices1b = Arrays.asList(slice1b2);
        FunctionWindowAggregateId functionWindowAggregateId1b = new FunctionWindowAggregateId(windowId1b, 0, 0);
        windowMerger.processPreAggregate(slices1b, functionWindowAggregateId1b);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final1bAll = windowMerger.triggerFinalWindow(functionWindowAggregateId1b);
        assertThat(final1bAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final1b = final1bAll.get(0);
        assertTrue(final1b.hasValue());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b1, slice1b2));
        Long finalAgg1b = windowMerger.lowerFinalValue(final1b);
        assertThat(finalAgg1b, equalTo(5L));

        DistributedSlice slice2c1 = slice1b1;
        DistributedSlice slice2c2 = slice1b2;
        List<DistributedSlice> slices2c = Collections.emptyList();
        FunctionWindowAggregateId functionWindowAggregateId2c = new FunctionWindowAggregateId(windowId2c, 0, 0);
        windowMerger.processPreAggregate(slices2c, functionWindowAggregateId2c);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2cAll = windowMerger.triggerFinalWindow(functionWindowAggregateId2c);
        assertThat(final2cAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2c = final2cAll.get(0);
        assertTrue(final2c.hasValue());
        assertThat(final2c.getAggValues().get(0), hasItems(slice2c1, slice2c2));
        Long finalAgg2c = windowMerger.lowerFinalValue(final2c);
        assertThat(finalAgg2c, equalTo(5L));

        DistributedSlice slice2d1 = slice1b2;
        DistributedSlice slice2d2 = new DistributedSlice(2000, 2500, 7, 3);
        List<DistributedSlice> slices2d = Arrays.asList(slice2d2);
        FunctionWindowAggregateId functionWindowAggregateId2d = new FunctionWindowAggregateId(windowId2d, 0, 0);
        windowMerger.processPreAggregate(slices2d, functionWindowAggregateId2d);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2dAll = windowMerger.triggerFinalWindow(functionWindowAggregateId2d);
        assertThat(final2dAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2d = final2dAll.get(0);
        assertTrue(final2d.hasValue());
        assertThat(final2d.getAggValues().get(0), hasItems(slice2d1, slice2d2));
        Long finalAgg2d = windowMerger.lowerFinalValue(final2d);
        assertThat(finalAgg2d, equalTo(6L));

        DistributedSlice slice1c1 = slice2d2;
        DistributedSlice slice1c2 = new DistributedSlice(2500, 3000, 1, 4);
        List<DistributedSlice> slices1c = Arrays.asList(slice1c2);
        FunctionWindowAggregateId functionWindowAggregateId1c = new FunctionWindowAggregateId(windowId1c, 0, 0);
        windowMerger.processPreAggregate(slices1c, functionWindowAggregateId1c);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final1cAll = windowMerger.triggerFinalWindow(functionWindowAggregateId1c);
        assertThat(final1cAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final1c = final1cAll.get(0);
        assertTrue(final1c.hasValue());
        assertThat(final1c.getAggValues().get(0), hasItems(slice1c1, slice1c2));
        Long finalAgg1c = windowMerger.lowerFinalValue(final1c);
        assertThat(finalAgg1c, equalTo(4L));

        DistributedSlice slice2e1 = slice1c1;
        DistributedSlice slice2e2 = slice1c2;
        List<DistributedSlice> slices2e = Arrays.asList();
        FunctionWindowAggregateId functionWindowAggregateId2e = new FunctionWindowAggregateId(windowId2e, 0, 0);
        windowMerger.processPreAggregate(slices2e, functionWindowAggregateId2e);
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final2eAll = windowMerger.triggerFinalWindow(functionWindowAggregateId2e);
        assertThat(final2eAll, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final2e = final2eAll.get(0);
        assertTrue(final2e.hasValue());
        assertThat(final2e.getAggValues().get(0), hasItems(slice2e1, slice2e2));
        Long finalAgg2e = windowMerger.lowerFinalValue(final2e);
        assertThat(finalAgg2e, equalTo(4L));
    }

    @Test
    void testTwoChildrenSessionMedian() {
        windows.add(new SessionWindow(WindowMeasure.Time, 100, 1));
        aggregateFunctions.add(new HolisticMergeWrapper(DistributedUtils.aggregateFunctionMedian()));
        int numChildren = 5;
        int childId1 = 0;
        int childId2 = 1;
        GlobalHolisticWindowMerger windowMerger = new GlobalHolisticWindowMerger(numChildren, windows, aggregateFunctions);
        windowMerger.initializeSessionState(Arrays.asList(childId1, childId2));

        WindowAggregateId windowId11 = new WindowAggregateId(1,   0, 110);
        WindowAggregateId windowId12 = new WindowAggregateId(1, 120, 270);
        WindowAggregateId windowId13 = new WindowAggregateId(1, 400, 510);
        WindowAggregateId windowId14 = new WindowAggregateId(1, 550, 650);

        WindowAggregateId windowId21 = new WindowAggregateId(1,   0, 130);
        WindowAggregateId windowId22 = new WindowAggregateId(1, 460, 690);
        WindowAggregateId windowId23 = new WindowAggregateId(1, 700, 850);

        DistributedSlice slice11a = new DistributedSlice(0,  5, 1, 2, 3);
        DistributedSlice slice11b = new DistributedSlice(5, 10, 4, 5, 6);
        List<DistributedSlice> slices11 = Arrays.asList(slice11a, slice11b);
        FunctionWindowAggregateId functionWindowAggId11 = new FunctionWindowAggregateId(windowId11, 0, childId1);
        windowMerger.processPreAggregate(slices11, functionWindowAggId11);
        Optional<FunctionWindowAggregateId> triggerId11 = windowMerger.checkWindowComplete(functionWindowAggId11);
        assertFalse(triggerId11.isPresent());

        FunctionWindowAggregateId sessionStart120 = FunctionWindowAggregateId.sessionStartId(1, 120, childId1, NO_KEY);
        Optional<FunctionWindowAggregateId> sessionStart120Opt = windowMerger.registerSessionStart(sessionStart120);
        assertFalse(sessionStart120Opt.isPresent());

        DistributedSlice slice21 = new DistributedSlice(0, 30, 1, 2, 3);
        List<DistributedSlice> slices21 = Arrays.asList(slice21);
        FunctionWindowAggregateId functionWindowAggId21 = new FunctionWindowAggregateId(windowId21, 0, childId2);
        windowMerger.processPreAggregate(slices21, functionWindowAggId21);
        Optional<FunctionWindowAggregateId> triggerId21 = windowMerger.checkWindowComplete(functionWindowAggId21);
        assertFalse(triggerId21.isPresent());

        FunctionWindowAggregateId sessionStart460 = FunctionWindowAggregateId.sessionStartId(1, 460, childId2, NO_KEY);
        Optional<FunctionWindowAggregateId> sessionStart460Opt = windowMerger.registerSessionStart(sessionStart460);
        assertFalse(sessionStart460Opt.isPresent());

        DistributedSlice slice12 = new DistributedSlice(120, 170, 0, 5, 10);
        List<DistributedSlice> slices12 = Arrays.asList(slice12);
        FunctionWindowAggregateId functionWindowAggId12 = new FunctionWindowAggregateId(windowId12, 0, childId1);
        windowMerger.processPreAggregate(slices12, functionWindowAggId12);
        Optional<FunctionWindowAggregateId> triggerId12 = windowMerger.checkWindowComplete(functionWindowAggId12);
        assertTrue(triggerId12.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final12All = windowMerger.triggerFinalWindow(triggerId12.get());
        assertThat(final12All, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final12 = final12All.get(0);
        assertTrue(final12.hasValue());
        assertThat(final12.getAggValues().get(0), hasItems(slice11a, slice11b, slice21, slice12));
        Long finalAgg12 = windowMerger.lowerFinalValue(final12);
        assertThat(finalAgg12, equalTo(3L));

        FunctionWindowAggregateId sessionStart400 = FunctionWindowAggregateId.sessionStartId(1, 400, childId1, NO_KEY);
        Optional<FunctionWindowAggregateId> sessionStart400Opt = windowMerger.registerSessionStart(sessionStart400);
        assertTrue(sessionStart400Opt.isPresent());
        assertThat(sessionStart400Opt.get(), equalTo(sessionStart400));

        DistributedSlice slice13 = new DistributedSlice(400, 410, 0, 5, 15);
        List<DistributedSlice> slices13 = Arrays.asList(slice13);
        FunctionWindowAggregateId functionWindowAggId13 = new FunctionWindowAggregateId(windowId13, 0, childId1);
        windowMerger.processPreAggregate(slices13, functionWindowAggId13);
        Optional<FunctionWindowAggregateId> triggerId13 = windowMerger.checkWindowComplete(functionWindowAggId13);
        assertFalse(triggerId13.isPresent());

        FunctionWindowAggregateId sessionStart550 = FunctionWindowAggregateId.sessionStartId(1, 550, childId1, NO_KEY);
        Optional<FunctionWindowAggregateId> sessionStart550Opt = windowMerger.registerSessionStart(sessionStart550);
        assertFalse(sessionStart550Opt.isPresent());

        DistributedSlice slice14 = new DistributedSlice(550, 550);
        List<DistributedSlice> slices14 = Arrays.asList(slice14);
        FunctionWindowAggregateId functionWindowAggId14 = new FunctionWindowAggregateId(windowId14, 0, childId1);
        windowMerger.processPreAggregate(slices14, functionWindowAggId14);
        Optional<FunctionWindowAggregateId> triggerId14 = windowMerger.checkWindowComplete(functionWindowAggId14);
        assertFalse(triggerId14.isPresent());

        FunctionWindowAggregateId sessionStart700 = FunctionWindowAggregateId.sessionStartId(1, 700, childId1, NO_KEY);
        Optional<FunctionWindowAggregateId> sessionStart700Opt = windowMerger.registerSessionStart(sessionStart700);
        assertFalse(sessionStart700Opt.isPresent());

        DistributedSlice slice22 = new DistributedSlice(460, 590, 15, 5, 20);
        List<DistributedSlice> slices22 = Arrays.asList(slice22);
        FunctionWindowAggregateId functionWindowAggId22 = new FunctionWindowAggregateId(windowId22, 0, childId2);
        windowMerger.processPreAggregate(slices22, functionWindowAggId22);
        Optional<FunctionWindowAggregateId> triggerId22 = windowMerger.checkWindowComplete(functionWindowAggId22);
        assertTrue(triggerId22.isPresent());
        List<DistributedAggregateWindowState<List<DistributedSlice>>> final22All = windowMerger.triggerFinalWindow(triggerId22.get());
        assertThat(final22All, hasSize(1));
        DistributedAggregateWindowState<List<DistributedSlice>> final22 = final22All.get(0);
        assertTrue(final22.hasValue());
        assertThat(final22.getAggValues().get(0), hasItems(slice13, slice22, slice14));
        Long finalAgg22 = windowMerger.lowerFinalValue(final22);
        assertThat(finalAgg22, equalTo(15L));
    }
}
