package com.github.lawben.disco.unit;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.HolisticWindowMerger;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.utils.MockSlice;
import com.github.lawben.disco.utils.WindowMergerTestBase;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HolisticWindowMergerTest extends WindowMergerTestBase {
    FunctionWindowAggregateId getFunctionWindowId(int childId) {
        return new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 0), 0, childId);
    }

    @Test
    public void testFinalOneChildMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        HolisticWindowMerger windowMerger = new HolisticWindowMerger();

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        Slice slice1 = new MockSlice(0, 1000, Arrays.asList(1));
        List<Slice> slices1 = Collections.singletonList(slice1);
        windowMerger.processPreAggregate(slices1, windowId1);
        AggregateWindow<List<Slice>> final1 = windowMerger.triggerFinalWindow(getFunctionWindowId(-1));
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().size());
        assertThat(final1.getAggValues().get(0), hasItems(slice1));

        Slice slice2 = new MockSlice(1000, 2000, Arrays.asList(1, 2, 3));
        List<Slice> slices2 = Collections.singletonList(slice2);
        windowMerger.processPreAggregate(slices2, windowId2);
        AggregateWindow<List<Slice>> final2 = windowMerger.triggerFinalWindow(getFunctionWindowId(-1));
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(1, final2.getAggValues().size());
        assertThat(final2.getAggValues().get(0), hasItems(slice2));

        Slice slice3 = new MockSlice(2000, 3000, Arrays.asList(1, 3, 5, 7, 9, 11));
        List<Slice> slices3 = Collections.singletonList(slice3);
        windowMerger.processPreAggregate(slices3, windowId3);
        AggregateWindow<List<Slice>> final3 = windowMerger.triggerFinalWindow(getFunctionWindowId(-1));
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(1, final3.getAggValues().size());
        assertThat(final3.getAggValues().get(0), hasItems(slice3));
    }

    @Test
    public void testFinalTwoChildrenMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        HolisticWindowMerger windowMerger = new HolisticWindowMerger();

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);


        Slice slice1a = new MockSlice(0, 1000, Arrays.asList(1));
        List<Slice> slices1a = Collections.singletonList(slice1a);
        windowMerger.processPreAggregate(slices1a, new FunctionWindowAggregateId(windowId1, 0, 0));
        AggregateWindow<List<Slice>> final1a = windowMerger.triggerFinalWindow(getFunctionWindowId(0));
        Assertions.assertTrue(final1a.hasValue());
        Assertions.assertEquals(1, final1a.getAggValues().size());
        assertThat(final1a.getAggValues().get(0), hasItems(slice1a));

        Slice slice1b = new MockSlice(0, 1000, Arrays.asList(2));
        List<Slice> slices1b = Collections.singletonList(slice1b);
        windowMerger.processPreAggregate(slices1b, new FunctionWindowAggregateId(windowId1, 0, 1));
        AggregateWindow<List<Slice>> final1b = windowMerger.triggerFinalWindow(getFunctionWindowId(1));
        Assertions.assertTrue(final1b.hasValue());
        Assertions.assertEquals(1, final1b.getAggValues().size());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b));

        Slice slice2a = new MockSlice(1000, 2000, Arrays.asList(2, 3, 4));
        List<Slice> slices2a = Collections.singletonList(slice2a);
        windowMerger.processPreAggregate(slices2a, new FunctionWindowAggregateId(windowId2, 0, 0));
        AggregateWindow<List<Slice>> final2a = windowMerger.triggerFinalWindow(getFunctionWindowId(0));
        Assertions.assertTrue(final2a.hasValue());
        Assertions.assertEquals(1, final2a.getAggValues().size());
        assertThat(final2a.getAggValues().get(0), hasItems(slice2a));

        Slice slice2b = new MockSlice(1000, 2000, Arrays.asList(1, 2, 3));
        List<Slice> slices2b = Collections.singletonList(slice2b);
        windowMerger.processPreAggregate(slices2b, new FunctionWindowAggregateId(windowId2, 0, 1));
        AggregateWindow<List<Slice>> final2b = windowMerger.triggerFinalWindow(getFunctionWindowId(1));
        Assertions.assertTrue(final2b.hasValue());
        Assertions.assertEquals(1, final2b.getAggValues().size());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b));

        Slice slice3a = new MockSlice(2000, 3000, Arrays.asList(1, 3, 5, 7, 9, 11));
        List<Slice> slices3a = Collections.singletonList(slice3a);
        windowMerger.processPreAggregate(slices3a, new FunctionWindowAggregateId(windowId3, 0, 0));
        AggregateWindow<List<Slice>> final3a = windowMerger.triggerFinalWindow(getFunctionWindowId(0));
        Assertions.assertTrue(final3a.hasValue());
        Assertions.assertEquals(1, final3a.getAggValues().size());
        assertThat(final3a.getAggValues().get(0), hasItems(slice3a));

        Slice slice3b = new MockSlice(2000, 3000, Arrays.asList(1, 3, 5, 7, 9, 11));
        List<Slice> slices3b = Collections.singletonList(slice3b);
        windowMerger.processPreAggregate(slices3b, new FunctionWindowAggregateId(windowId3, 0, 1));
        AggregateWindow<List<Slice>> final3b = windowMerger.triggerFinalWindow(getFunctionWindowId(1));
        Assertions.assertTrue(final3b.hasValue());
        Assertions.assertEquals(1, final3b.getAggValues().size());
        assertThat(final3b.getAggValues().get(0), hasItems(slice3b));
    }


    @Test
    public void testFinalOneChildTwoWindowsMedian() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        HolisticWindowMerger windowMerger = new HolisticWindowMerger();

        FunctionWindowAggregateId functionId = getFunctionWindowId(0);

        WindowAggregateId windowId1a = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId1b = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId1c = new WindowAggregateId(1, 2000, 3000);

        WindowAggregateId windowId2a = new WindowAggregateId(2,    0, 1000);
        WindowAggregateId windowId2b = new WindowAggregateId(2,  500, 1500);
        WindowAggregateId windowId2c = new WindowAggregateId(2, 1000, 2000);
        WindowAggregateId windowId2d = new WindowAggregateId(2, 1500, 2500);
        WindowAggregateId windowId2e = new WindowAggregateId(2, 2000, 3000);

        Slice slice1a1 = new MockSlice(0, 500, Arrays.asList(1));
        Slice slice1a2 = new MockSlice(500, 1000, Arrays.asList(2));
        List<Slice> slices1a = Arrays.asList(slice1a1, slice1a2);
        windowMerger.processPreAggregate(slices1a, new FunctionWindowAggregateId(windowId1a, 0, 0));
        AggregateWindow<List<Slice>> final1a = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final1a.hasValue());
        assertThat(final1a.getAggValues().get(0), hasItems(slice1a1, slice1a2));

        Slice slice2a1 = slice1a1;
        Slice slice2a2 = slice1a2;
        List<Slice> slices2a = Arrays.asList(slice2a1, slice2a2);
        windowMerger.processPreAggregate(slices2a, new FunctionWindowAggregateId(windowId2a, 0, 0));
        AggregateWindow<List<Slice>> final2a = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final2a.hasValue());
        assertThat(final2a.getAggValues().get(0), empty());

        Slice slice2b1 = slice2a2;
        Slice slice2b2 = new MockSlice(1000, 1500, Arrays.asList(2, 4));
        List<Slice> slices2b = Arrays.asList(slice2b1, slice2b2);
        windowMerger.processPreAggregate(slices2b, new FunctionWindowAggregateId(windowId2b, 0, 0));
        AggregateWindow<List<Slice>> final2b = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final2b.hasValue());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b2));

        Slice slice1b1 = slice2b2;
        Slice slice1b2 = new MockSlice(1500, 2000, Arrays.asList(5, 5, 5));
        List<Slice> slices1b = Arrays.asList(slice1b1, slice1b2);
        windowMerger.processPreAggregate(slices1b, new FunctionWindowAggregateId(windowId1b, 0, 0));
        AggregateWindow<List<Slice>> final1b = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final1b.hasValue());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b2));
        
        Slice slice2c1 = slice1b1;
        Slice slice2c2 = slice1b2;
        List<Slice> slices2c = Arrays.asList(slice2c1, slice2c2);
        windowMerger.processPreAggregate(slices2c, new FunctionWindowAggregateId(windowId2c, 0, 0));
        AggregateWindow<List<Slice>> final2c = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final2c.hasValue());
        assertThat(final2c.getAggValues().get(0), empty());

        Slice slice2d1 = slice1b2;
        Slice slice2d2 = new MockSlice(2000, 2500, Arrays.asList(7, 3));
        List<Slice> slices2d = Arrays.asList(slice2d1, slice2d2);
        windowMerger.processPreAggregate(slices2d, new FunctionWindowAggregateId(windowId2d, 0, 0));
        AggregateWindow<List<Slice>> final2d = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final2d.hasValue());
        assertThat(final2d.getAggValues().get(0), hasItems(slice2d2));

        Slice slice1c1 = slice2d2;
        Slice slice1c2 = new MockSlice(2500, 3000, Arrays.asList(1, 4));
        List<Slice> slices1c = Arrays.asList(slice1c1, slice1c2);
        windowMerger.processPreAggregate(slices1c, new FunctionWindowAggregateId(windowId1c, 0, 0));
        AggregateWindow<List<Slice>> final1c = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final1c.hasValue());
        assertThat(final1c.getAggValues().get(0), hasItems(slice1c2));

        Slice slice2e1 = slice1c1;
        Slice slice2e2 = slice1c2;
        List<Slice> slices2e = Arrays.asList(slice2e1, slice2e2);
        windowMerger.processPreAggregate(slices2e, new FunctionWindowAggregateId(windowId2e, 0, 0));
        AggregateWindow<List<Slice>> final2e = windowMerger.triggerFinalWindow(functionId);
        Assertions.assertTrue(final2e.hasValue());
        assertThat(final2e.getAggValues().get(0), empty());

    }
}
