package com.github.lawben.disco.unit;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticMergeWrapper;
import com.github.lawben.disco.merge.LocalHolisticWindowMerger;
import com.github.lawben.disco.utils.WindowMergerTestBase;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class LocalHolisticWindowMergerTest extends WindowMergerTestBase {
    @Test
    void testFinalOneStreamMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticMergeWrapper());
        int numStreams = 1;
        LocalHolisticWindowMerger windowMerger = new LocalHolisticWindowMerger(numStreams, windows);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        Slice slice1 = new DistributedSlice(0, 1000, 1);
        List<Slice> slices1 = Collections.singletonList(slice1);
        windowMerger.processPreAggregate(slices1, windowId1);
        DistributedAggregateWindowState<List<Slice>> final1 = windowMerger.triggerFinalWindow(windowId1).get(0);
        assertTrue(final1.hasValue());
        assertEquals(1, final1.getAggValues().size());
        assertThat(final1.getAggValues().get(0), hasItems(slice1));

        Slice slice2 = new DistributedSlice(1000, 2000, 1, 2, 3);
        List<Slice> slices2 = Collections.singletonList(slice2);
        windowMerger.processPreAggregate(slices2, windowId2);
        DistributedAggregateWindowState<List<Slice>> final2 = windowMerger.triggerFinalWindow(windowId2).get(0);
        assertTrue(final2.hasValue());
        assertEquals(1, final2.getAggValues().size());
        assertThat(final2.getAggValues().get(0), hasItems(slice2));

        Slice slice3 = new DistributedSlice(2000, 3000, 1, 3, 5, 7, 9, 11);
        List<Slice> slices3 = Collections.singletonList(slice3);
        windowMerger.processPreAggregate(slices3, windowId3);
        DistributedAggregateWindowState<List<Slice>> final3 = windowMerger.triggerFinalWindow(windowId3).get(0);
        assertTrue(final3.hasValue());
        assertEquals(1, final3.getAggValues().size());
        assertThat(final3.getAggValues().get(0), hasItems(slice3));
    }

    @Test
    void testFinalTwoStreamsMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new HolisticMergeWrapper());
        int numStreams = 2;
        LocalHolisticWindowMerger windowMerger = new LocalHolisticWindowMerger(numStreams, windows);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        Slice slice1a = new DistributedSlice(0, 1000, 1);
        List<Slice> slices1a = Collections.singletonList(slice1a);
        FunctionWindowAggregateId functionWindowAggId1 = new FunctionWindowAggregateId(windowId1, 0, 0, 0);
        windowMerger.processPreAggregate(slices1a, functionWindowAggId1);
        DistributedAggregateWindowState<List<Slice>> final1a = windowMerger.triggerFinalWindow(functionWindowAggId1).get(0);
        assertTrue(final1a.hasValue());
        assertEquals(1, final1a.getAggValues().size());
        assertThat(final1a.getAggValues().get(0), hasItems(slice1a));

        Slice slice1b = new DistributedSlice(0, 1000, 2);
        List<Slice> slices1b = Collections.singletonList(slice1b);
        FunctionWindowAggregateId functionWindowAggId2 = new FunctionWindowAggregateId(windowId1, 0, 0, 1);
        windowMerger.processPreAggregate(slices1b, functionWindowAggId2);
        DistributedAggregateWindowState<List<Slice>> final1b = windowMerger.triggerFinalWindow(functionWindowAggId2).get(0);
        assertTrue(final1b.hasValue());
        assertEquals(1, final1b.getAggValues().size());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b));

        Slice slice2a = new DistributedSlice(1000, 2000, 2, 3, 4);
        List<Slice> slices2a = Collections.singletonList(slice2a);
        FunctionWindowAggregateId functionWindowAggId3 = new FunctionWindowAggregateId(windowId2, 0, 0, 0);
        windowMerger.processPreAggregate(slices2a, functionWindowAggId3);
        DistributedAggregateWindowState<List<Slice>> final2a = windowMerger.triggerFinalWindow(functionWindowAggId3).get(0);
        assertTrue(final2a.hasValue());
        assertEquals(1, final2a.getAggValues().size());
        assertThat(final2a.getAggValues().get(0), hasItems(slice2a));

        Slice slice2b = new DistributedSlice(1000, 2000, 1, 2, 3);
        List<Slice> slices2b = Collections.singletonList(slice2b);
        FunctionWindowAggregateId functionWindowAggId4 = new FunctionWindowAggregateId(windowId2, 0, 0, 1);
        windowMerger.processPreAggregate(slices2b, functionWindowAggId4);
        DistributedAggregateWindowState<List<Slice>> final2b = windowMerger.triggerFinalWindow(functionWindowAggId4).get(0);
        assertTrue(final2b.hasValue());
        assertEquals(1, final2b.getAggValues().size());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b));

        Slice slice3a = new DistributedSlice(2000, 3000, 1, 3, 5, 7, 9, 11);
        List<Slice> slices3a = Collections.singletonList(slice3a);
        FunctionWindowAggregateId functionWindowAggId5 = new FunctionWindowAggregateId(windowId3, 0, 0, 0);
        windowMerger.processPreAggregate(slices3a, functionWindowAggId5);
        DistributedAggregateWindowState<List<Slice>> final3a = windowMerger.triggerFinalWindow(functionWindowAggId5).get(0);
        assertTrue(final3a.hasValue());
        assertEquals(1, final3a.getAggValues().size());
        assertThat(final3a.getAggValues().get(0), hasItems(slice3a));

        Slice slice3b = new DistributedSlice(2000, 3000, 1, 3, 5, 7, 9, 11);
        List<Slice> slices3b = Collections.singletonList(slice3b);
        FunctionWindowAggregateId functionWindowAggId6 = new FunctionWindowAggregateId(windowId3, 0, 0, 1);
        windowMerger.processPreAggregate(slices3b, functionWindowAggId6);
        DistributedAggregateWindowState<List<Slice>> final3b = windowMerger.triggerFinalWindow(functionWindowAggId6).get(0);
        assertTrue(final3b.hasValue());
        assertEquals(1, final3b.getAggValues().size());
        assertThat(final3b.getAggValues().get(0), hasItems(slice3b));
    }

    @Test
    void testFinalOneStreamTwoWindowsMedian() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(new HolisticMergeWrapper());
        int numStreams = 1;
        LocalHolisticWindowMerger windowMerger = new LocalHolisticWindowMerger(numStreams, windows);

        WindowAggregateId windowId1a = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId1b = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId1c = new WindowAggregateId(1, 2000, 3000);

        WindowAggregateId windowId2a = new WindowAggregateId(2,    0, 1000);
        WindowAggregateId windowId2b = new WindowAggregateId(2,  500, 1500);
        WindowAggregateId windowId2c = new WindowAggregateId(2, 1000, 2000);
        WindowAggregateId windowId2d = new WindowAggregateId(2, 1500, 2500);
        WindowAggregateId windowId2e = new WindowAggregateId(2, 2000, 3000);

        Slice slice1a1 = new DistributedSlice(0, 500, 1);
        Slice slice1a2 = new DistributedSlice(500, 1000, 2);
        List<Slice> slices1a = Arrays.asList(slice1a1, slice1a2);
        FunctionWindowAggregateId functionWindowAggregateId1a = new FunctionWindowAggregateId(windowId1a, 0, 0);
        windowMerger.processPreAggregate(slices1a, functionWindowAggregateId1a);
        DistributedAggregateWindowState<List<Slice>> final1a = windowMerger.triggerFinalWindow(functionWindowAggregateId1a).get(0);
        assertTrue(final1a.hasValue());
        assertThat(final1a.getAggValues().get(0), hasItems(slice1a1, slice1a2));

        Slice slice2a1 = slice1a1;
        Slice slice2a2 = slice1a2;
        List<Slice> slices2a = Arrays.asList(slice2a1, slice2a2);
        FunctionWindowAggregateId functionWindowAggregateId2a = new FunctionWindowAggregateId(windowId2a, 0, 0);
        windowMerger.processPreAggregate(slices2a, functionWindowAggregateId2a);
        DistributedAggregateWindowState<List<Slice>> final2a = windowMerger.triggerFinalWindow(functionWindowAggregateId2a).get(0);
        assertTrue(final2a.hasValue());
        assertThat(final2a.getAggValues().get(0), empty());

        Slice slice2b1 = slice2a2;
        Slice slice2b2 = new DistributedSlice(1000, 1500, 2, 4);
        List<Slice> slices2b = Arrays.asList(slice2b1, slice2b2);
        FunctionWindowAggregateId functionWindowAggregateId2b = new FunctionWindowAggregateId(windowId2b, 0, 0);
        windowMerger.processPreAggregate(slices2b, functionWindowAggregateId2b);
        DistributedAggregateWindowState<List<Slice>> final2b = windowMerger.triggerFinalWindow(functionWindowAggregateId2b).get(0);
        assertTrue(final2b.hasValue());
        assertThat(final2b.getAggValues().get(0), hasItems(slice2b2));

        Slice slice1b1 = slice2b2;
        Slice slice1b2 = new DistributedSlice(1500, 2000, 5, 5, 5);
        List<Slice> slices1b = Arrays.asList(slice1b1, slice1b2);
        FunctionWindowAggregateId functionWindowAggregateId1b = new FunctionWindowAggregateId(windowId1b, 0, 0);
        windowMerger.processPreAggregate(slices1b, functionWindowAggregateId1b);
        DistributedAggregateWindowState<List<Slice>> final1b = windowMerger.triggerFinalWindow(functionWindowAggregateId1b).get(0);
        assertTrue(final1b.hasValue());
        assertThat(final1b.getAggValues().get(0), hasItems(slice1b2));

        Slice slice2c1 = slice1b1;
        Slice slice2c2 = slice1b2;
        List<Slice> slices2c = Arrays.asList(slice2c1, slice2c2);
        FunctionWindowAggregateId functionWindowAggregateId2c = new FunctionWindowAggregateId(windowId2c, 0, 0);
        windowMerger.processPreAggregate(slices2c, functionWindowAggregateId2c);
        DistributedAggregateWindowState<List<Slice>> final2c = windowMerger.triggerFinalWindow(functionWindowAggregateId2c).get(0);
        assertTrue(final2c.hasValue());
        assertThat(final2c.getAggValues().get(0), empty());

        Slice slice2d1 = slice1b2;
        Slice slice2d2 = new DistributedSlice(2000, 2500, 7, 3);
        List<Slice> slices2d = Arrays.asList(slice2d1, slice2d2);
        FunctionWindowAggregateId functionWindowAggregateId2d = new FunctionWindowAggregateId(windowId2d, 0, 0);
        windowMerger.processPreAggregate(slices2d, functionWindowAggregateId2d);
        DistributedAggregateWindowState<List<Slice>> final2d = windowMerger.triggerFinalWindow(functionWindowAggregateId2d).get(0);
        assertTrue(final2d.hasValue());
        assertThat(final2d.getAggValues().get(0), hasItems(slice2d2));

        Slice slice1c1 = slice2d2;
        Slice slice1c2 = new DistributedSlice(2500, 3000, 1, 4);
        List<Slice> slices1c = Arrays.asList(slice1c1, slice1c2);
        FunctionWindowAggregateId functionWindowAggregateId1c = new FunctionWindowAggregateId(windowId1c, 0, 0);
        windowMerger.processPreAggregate(slices1c, functionWindowAggregateId1c);
        DistributedAggregateWindowState<List<Slice>> final1c = windowMerger.triggerFinalWindow(functionWindowAggregateId1c).get(0);
        assertTrue(final1c.hasValue());
        assertThat(final1c.getAggValues().get(0), hasItems(slice1c2));

        Slice slice2e1 = slice1c1;
        Slice slice2e2 = slice1c2;
        List<Slice> slices2e = Arrays.asList(slice2e1, slice2e2);
        FunctionWindowAggregateId functionWindowAggregateId2e = new FunctionWindowAggregateId(windowId2e, 0, 0);
        windowMerger.processPreAggregate(slices2e, functionWindowAggregateId2e);
        DistributedAggregateWindowState<List<Slice>> final2e = windowMerger.triggerFinalWindow(functionWindowAggregateId2e).get(0);
        assertTrue(final2e.hasValue());
        assertThat(final2e.getAggValues().get(0), empty());
    }

    @Test
    void testTwoStreamsSessionMedian() {
        windows.add(new SessionWindow(WindowMeasure.Time, 100, 1));
        aggregateFunctions.add(new HolisticMergeWrapper());
        int numStreams = 2;
        LocalHolisticWindowMerger windowMerger = new LocalHolisticWindowMerger(numStreams, windows);

        int streamId1 = 0;
        int streamId2 = 1;

        WindowAggregateId windowId11 = new WindowAggregateId(1,   0, 110);
        WindowAggregateId windowId12 = new WindowAggregateId(1, 120, 270);
        WindowAggregateId windowId13 = new WindowAggregateId(1, 400, 510);
        WindowAggregateId windowId14 = new WindowAggregateId(1, 550, 650);

        WindowAggregateId windowId21 = new WindowAggregateId(1,   0, 130);
        WindowAggregateId windowId22 = new WindowAggregateId(1, 460, 690);
        WindowAggregateId windowId23 = new WindowAggregateId(1, 700, 850);

        Slice slice11a = new DistributedSlice(0,  5, 1, 2, 3);
        Slice slice11b = new DistributedSlice(5, 10, 4, 5, 6);
        List<Slice> slices11 = Arrays.asList(slice11a, slice11b);
        FunctionWindowAggregateId functionWindowAggId11 = new FunctionWindowAggregateId(windowId11, 0, 0, streamId1);
        windowMerger.processPreAggregate(slices11, functionWindowAggId11);
        DistributedAggregateWindowState<List<Slice>> final11 = windowMerger.triggerFinalWindow(functionWindowAggId11).get(0);
        assertTrue(final11.hasValue());
        assertEquals(1, final11.getAggValues().size());
        assertThat(final11.getAggValues().get(0), hasItems(slice11a, slice11b));

        Slice slice21 = new DistributedSlice(0, 30, 1, 2, 3);
        List<Slice> slices21 = Arrays.asList(slice21);
        FunctionWindowAggregateId functionWindowAggId21 = new FunctionWindowAggregateId(windowId21, 0, 0, streamId2);
        windowMerger.processPreAggregate(slices21, functionWindowAggId21);
        DistributedAggregateWindowState<List<Slice>> final21 = windowMerger.triggerFinalWindow(functionWindowAggId21).get(0);
        assertTrue(final21.hasValue());
        assertEquals(1, final21.getAggValues().size());
        assertThat(final21.getAggValues().get(0), hasItems(slice21));

        Slice slice12 = new DistributedSlice(120, 170, 0, 5, 10);
        List<Slice> slices12 = Arrays.asList(slice12);
        FunctionWindowAggregateId functionWindowAggId12 = new FunctionWindowAggregateId(windowId12, 0, 0, streamId1);
        windowMerger.processPreAggregate(slices12, functionWindowAggId12);
        DistributedAggregateWindowState<List<Slice>> final12 = windowMerger.triggerFinalWindow(functionWindowAggId12).get(0);
        assertTrue(final12.hasValue());
        assertEquals(1, final12.getAggValues().size());
        assertThat(final12.getAggValues().get(0), hasItems(slice12));

        Slice slice13 = new DistributedSlice(400, 410, 0, 5, 15);
        List<Slice> slices13 = Arrays.asList(slice13);
        FunctionWindowAggregateId functionWindowAggId13 = new FunctionWindowAggregateId(windowId13, 0, 0, streamId1);
        windowMerger.processPreAggregate(slices13, functionWindowAggId13);
        DistributedAggregateWindowState<List<Slice>> final13 = windowMerger.triggerFinalWindow(functionWindowAggId13).get(0);
        assertTrue(final13.hasValue());
        assertEquals(1, final13.getAggValues().size());
        assertThat(final13.getAggValues().get(0), hasItems(slice13));

        Slice slice22 = new DistributedSlice(460, 590, 15, 5, 15);
        List<Slice> slices22 = Arrays.asList(slice22);
        FunctionWindowAggregateId functionWindowAggId22 = new FunctionWindowAggregateId(windowId22, 0, 0, streamId2);
        windowMerger.processPreAggregate(slices22, functionWindowAggId22);
        DistributedAggregateWindowState<List<Slice>> final22 = windowMerger.triggerFinalWindow(functionWindowAggId22).get(0);
        assertTrue(final22.hasValue());
        assertEquals(1, final22.getAggValues().size());
        assertThat(final22.getAggValues().get(0), hasItems(slice22));

        Slice slice14 = new DistributedSlice(550, 550);
        List<Slice> slices14 = Arrays.asList(slice14);
        FunctionWindowAggregateId functionWindowAggId14 = new FunctionWindowAggregateId(windowId14, 0, 0, streamId1);
        windowMerger.processPreAggregate(slices14, functionWindowAggId14);
        DistributedAggregateWindowState<List<Slice>> final14 = windowMerger.triggerFinalWindow(functionWindowAggId14).get(0);
        assertTrue(final14.hasValue());
        assertEquals(1, final14.getAggValues().size());
        assertThat(final14.getAggValues().get(0), hasItems(slice14));

        Slice slice23 = new DistributedSlice(700, 750, 100, 0);
        List<Slice> slices23 = Arrays.asList(slice23);
        FunctionWindowAggregateId functionWindowAggId23 = new FunctionWindowAggregateId(windowId23, 0, 0, streamId2);
        windowMerger.processPreAggregate(slices23, functionWindowAggId23);
        DistributedAggregateWindowState<List<Slice>> final23 = windowMerger.triggerFinalWindow(functionWindowAggId23).get(0);
        assertTrue(final23.hasValue());
        assertEquals(1, final23.getAggValues().size());
        assertThat(final23.getAggValues().get(0), hasItems(slice23));
    }
}
