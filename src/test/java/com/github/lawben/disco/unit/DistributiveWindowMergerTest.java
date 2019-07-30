package com.github.lawben.disco.unit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.DistributiveWindowMerger;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.utils.WindowMergerTestBase;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistributiveWindowMergerTest extends WindowMergerTestBase {
    private AggregateFunction sumFunction;

    @Override
    @BeforeEach
    public void setup() {
        super.setup();
        this.sumFunction = DistributedUtils.aggregateFunctionSum();
    }

    @Test
    void testFinalTwoChildrenTwoWindows() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        DistributiveWindowMerger<Integer> windowMerger = new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1a = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2a = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3a = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        FunctionWindowAggregateId windowId1b = defaultFnWindowAggId(new WindowAggregateId(2,    0, 1000));
        FunctionWindowAggregateId windowId2b = defaultFnWindowAggId(new WindowAggregateId(2,  500, 1500));
        FunctionWindowAggregateId windowId3b = defaultFnWindowAggId(new WindowAggregateId(2, 1000, 2000));
        FunctionWindowAggregateId windowId4b = defaultFnWindowAggId(new WindowAggregateId(2, 1500, 2500));
        FunctionWindowAggregateId windowId5b = defaultFnWindowAggId(new WindowAggregateId(2, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1a);
        windowMerger.processPreAggregate(2, windowId1a);
        List<DistributedAggregateWindowState<Integer>> final1aAll = windowMerger.triggerFinalWindow(windowId1a);
        Assertions.assertEquals(final1aAll.size(), 1);
        DistributedAggregateWindowState<Integer> final1a = final1aAll.get(0);
        assertTrue(final1a.hasValue());
        Assertions.assertEquals(3, final1a.getAggValues().get(0));

        windowMerger.processPreAggregate(4, windowId1b);
        windowMerger.processPreAggregate(5, windowId1b);
        List<DistributedAggregateWindowState<Integer>> final1bAll = windowMerger.triggerFinalWindow(windowId1b);
        Assertions.assertEquals(final1bAll.size(), 1);
        DistributedAggregateWindowState<Integer> final1b = final1bAll.get(0);
        assertTrue(final1b.hasValue());
        Assertions.assertEquals(9, final1b.getAggValues().get(0));

        windowMerger.processPreAggregate(5, windowId2b);
        windowMerger.processPreAggregate(6, windowId2b);
        List<DistributedAggregateWindowState<Integer>> final2bAll = windowMerger.triggerFinalWindow(windowId2b);
        Assertions.assertEquals(final2bAll.size(), 1);
        DistributedAggregateWindowState<Integer> final2b = final2bAll.get(0);
        assertTrue(final2b.hasValue());
        Assertions.assertEquals(11, final2b.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2a);
        windowMerger.processPreAggregate(3, windowId2a);
        List<DistributedAggregateWindowState<Integer>> final2aAll = windowMerger.triggerFinalWindow(windowId2a);
        Assertions.assertEquals(final2aAll.size(), 1);
        DistributedAggregateWindowState<Integer> final2a = final2aAll.get(0);
        assertTrue(final2a.hasValue());
        Assertions.assertEquals(5, final2a.getAggValues().get(0));

        windowMerger.processPreAggregate(6, windowId3b);
        windowMerger.processPreAggregate(7, windowId3b);
        List<DistributedAggregateWindowState<Integer>> final3bAll = windowMerger.triggerFinalWindow(windowId3b);
        Assertions.assertEquals(final3bAll.size(), 1);
        DistributedAggregateWindowState<Integer> final3b = final3bAll.get(0);
        assertTrue(final3b.hasValue());
        Assertions.assertEquals(13, final3b.getAggValues().get(0));

        windowMerger.processPreAggregate(7, windowId4b);
        windowMerger.processPreAggregate(8, windowId4b);
        List<DistributedAggregateWindowState<Integer>> final4bAll = windowMerger.triggerFinalWindow(windowId4b);
        Assertions.assertEquals(final4bAll.size(), 1);
        DistributedAggregateWindowState<Integer> final4b = final4bAll.get(0);
        assertTrue(final4b.hasValue());
        Assertions.assertEquals(15, final4b.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3a);
        windowMerger.processPreAggregate(4, windowId3a);
        List<DistributedAggregateWindowState<Integer>> final3aAll = windowMerger.triggerFinalWindow(windowId3a);
        Assertions.assertEquals(final3aAll.size(), 1);
        DistributedAggregateWindowState<Integer> final3a = final3aAll.get(0);
        assertTrue(final3a.hasValue());
        Assertions.assertEquals(7, final3a.getAggValues().get(0));

        windowMerger.processPreAggregate(8, windowId5b);
        windowMerger.processPreAggregate(9, windowId5b);
        List<DistributedAggregateWindowState<Integer>> final5bAll = windowMerger.triggerFinalWindow(windowId5b);
        Assertions.assertEquals(final5bAll.size(), 1);
        DistributedAggregateWindowState<Integer> final5b = final5bAll.get(0);
        assertTrue(final5b.hasValue());
        Assertions.assertEquals(17, final5b.getAggValues().get(0));
    }

    @Test
    void testFinalOneChild() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        DistributiveWindowMerger<Integer> windowMerger = new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1);
        List<DistributedAggregateWindowState<Integer>> final1All = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertEquals(final1All.size(), 1);
        DistributedAggregateWindowState<Integer> final1 = final1All.get(0);
        assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2);
        List<DistributedAggregateWindowState<Integer>> final2All = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertEquals(final2All.size(), 1);
        DistributedAggregateWindowState<Integer> final2 = final2All.get(0);
        assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3);
        List<DistributedAggregateWindowState<Integer>> final3All = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertEquals(final3All.size(), 1);
        DistributedAggregateWindowState<Integer> final3 = final3All.get(0);
        assertTrue(final3.hasValue());
        Assertions.assertEquals(3, final3.getAggValues().get(0));
    }

    @Test
    void testFinalTwoChildren() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 2;
        DistributiveWindowMerger<Integer> windowMerger = new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        List<DistributedAggregateWindowState<Integer>> final1All = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertEquals(final1All.size(), 1);
        DistributedAggregateWindowState<Integer> final1 = final1All.get(0);
        assertTrue(final1.hasValue());
        Assertions.assertEquals(3, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        List<DistributedAggregateWindowState<Integer>> final2All = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertEquals(final2All.size(), 1);
        DistributedAggregateWindowState<Integer> final2 = final2All.get(0);
        assertTrue(final2.hasValue());
        Assertions.assertEquals(5, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        List<DistributedAggregateWindowState<Integer>> final3All = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertEquals(final3All.size(), 1);
        DistributedAggregateWindowState<Integer> final3 = final3All.get(0);
        assertTrue(final3.hasValue());
        Assertions.assertEquals(7, final3.getAggValues().get(0));
    }

    @Test
    void testFinalFiveChildren() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 5;
        DistributiveWindowMerger<Integer> windowMerger = new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        windowMerger.processPreAggregate(3, windowId1);
        windowMerger.processPreAggregate(4, windowId1);
        windowMerger.processPreAggregate(5, windowId1);
        List<DistributedAggregateWindowState<Integer>> final1All = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertEquals(final1All.size(), 1);
        DistributedAggregateWindowState<Integer> final1 = final1All.get(0);
        assertTrue(final1.hasValue());
        Assertions.assertEquals(15, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        windowMerger.processPreAggregate(4, windowId2);
        windowMerger.processPreAggregate(5, windowId2);
        windowMerger.processPreAggregate(6, windowId2);
        List<DistributedAggregateWindowState<Integer>> final2All = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertEquals(final2All.size(), 1);
        DistributedAggregateWindowState<Integer> final2 = final2All.get(0);
        assertTrue(final2.hasValue());
        Assertions.assertEquals(20, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        windowMerger.processPreAggregate(5, windowId3);
        windowMerger.processPreAggregate(6, windowId3);
        windowMerger.processPreAggregate(7, windowId3);
        List<DistributedAggregateWindowState<Integer>> final3All = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertEquals(final3All.size(), 1);
        DistributedAggregateWindowState<Integer> final3 = final3All.get(0);
        assertTrue(final3.hasValue());
        Assertions.assertEquals(25, final3.getAggValues().get(0));
    }

    @Test
    void testSessionOneChild() {
        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
        windows.add(sessionWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        DistributiveWindowMerger<Integer> windowMerger =
                new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,  10, 110));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 120, 320));

        windowMerger.processPreAggregate(5, windowId1);
        Optional<FunctionWindowAggregateId> triggerId1 = windowMerger.checkWindowComplete(windowId1);
        assertTrue(triggerId1.isPresent());
        List<DistributedAggregateWindowState<Integer>> final1All = windowMerger.triggerFinalWindow(triggerId1.get());
        assertThat(final1All, hasSize(1));
        DistributedAggregateWindowState<Integer> final1 = final1All.get(0);
        assertThat(final1.getFunctionWindowId(), equalTo(windowId1));
        assertTrue(final1.hasValue());
        assertThat(final1.getAggValues().get(0), equalTo(5));

        windowMerger.processPreAggregate(20, windowId2);
        Optional<FunctionWindowAggregateId> triggerId2 = windowMerger.checkWindowComplete(windowId2);
        assertTrue(triggerId2.isPresent());
        List<DistributedAggregateWindowState<Integer>> final2All = windowMerger.triggerFinalWindow(triggerId2.get());
        assertThat(final2All, hasSize(1));
        DistributedAggregateWindowState<Integer> final2 = final2All.get(0);
        assertThat(final2.getFunctionWindowId(), equalTo(windowId2));
        assertTrue(final2.hasValue());
        assertThat(final2.getAggValues().get(0), equalTo(20));
    }

    @Test
    void testSessionFourChildren() {
//        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
//        windows.add(sessionWindow);
//        aggregateFunctions.add(sumFunction);
//        int numChildren = 4;
//        DistributiveWindowMerger<Integer> windowMerger = new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);
//
//        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,  10, 110));
//        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1,  20, 120));
//        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 110, 210));
//        FunctionWindowAggregateId windowId4 = defaultFnWindowAggId(new WindowAggregateId(1, 220, 320));
//
//        Optional<FunctionWindowAggregateId> triggerId1 = windowMerger.processPreAggregate(5, windowId1);
//        Optional<FunctionWindowAggregateId> triggerId2 = windowMerger.processPreAggregate(10, windowId2);
//        Optional<FunctionWindowAggregateId> triggerId3 = windowMerger.processPreAggregate(15, windowId3);
//        Optional<FunctionWindowAggregateId> triggerId4 = windowMerger.processPreAggregate(20, windowId4);
//
//        Assertions.assertFalse(triggerId1.isPresent());
//        Assertions.assertFalse(triggerId2.isPresent());
//        Assertions.assertFalse(triggerId3.isPresent());
//
//        Assertions.assertTrue(triggerId4.isPresent());
//        FunctionWindowAggregateId expectedTriggerId = defaultFnWindowAggId(new WindowAggregateId(1, 10, 210));
//        Assertions.assertEquals(expectedTriggerId, triggerId4.get());
//
//        List<DistributedAggregateWindowState<Integer>> finalAggAll = windowMerger.triggerFinalWindow(triggerId4.get());
//        Assertions.assertEquals(finalAggAll.size(), 1);
//        DistributedAggregateWindowState<Integer> finalAgg = finalAggAll.get(0);
//        Assertions.assertTrue(finalAgg.hasValue());
//        Assertions.assertEquals(30, finalAgg.getAggValues().get(0));
        fail();
    }

    @Test
    void testSessionOneChildTwoAggFns() {
//        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
//        windows.add(sessionWindow);
//        aggregateFunctions.add(sumFunction);
//        aggregateFunctions.add(sumFunction);
//        int numChildren = 1;
//        DistributiveWindowMerger<Integer> windowMerger = new DistributiveWindowMerger<>(numChildren, windows, aggregateFunctions);
//
//        WindowAggregateId windowId1 = new WindowAggregateId(1,  10, 110);
//        WindowAggregateId windowId2 = new WindowAggregateId(1, 120, 320);
//
//        FunctionWindowAggregateId functionWindowId10 = new FunctionWindowAggregateId(windowId1, 0);
//        FunctionWindowAggregateId functionWindowId20 = new FunctionWindowAggregateId(windowId2, 0);
//
//        FunctionWindowAggregateId functionWindowId11 = new FunctionWindowAggregateId(windowId1, 1);
//        FunctionWindowAggregateId functionWindowId21 = new FunctionWindowAggregateId(windowId2, 1);
//
//        Optional<FunctionWindowAggregateId> triggerId10 = windowMerger.processPreAggregate(5, functionWindowId10);
//        Optional<FunctionWindowAggregateId> triggerId11 = windowMerger.processPreAggregate(10, functionWindowId11);
//
//        Optional<FunctionWindowAggregateId> triggerId20 = windowMerger.processPreAggregate(15, functionWindowId20);
//        Optional<FunctionWindowAggregateId> triggerId21 = windowMerger.processPreAggregate(20, functionWindowId21);
//
//        FunctionWindowAggregateId expectedTriggerId0 = new FunctionWindowAggregateId(new WindowAggregateId(1, 10, 110), 0);
//        FunctionWindowAggregateId expectedTriggerId1 = new FunctionWindowAggregateId(new WindowAggregateId(1, 10, 110), 1);
//        Assertions.assertEquals(expectedTriggerId0, triggerId20.get());
//        Assertions.assertEquals(expectedTriggerId1, triggerId21.get());
//
//        List<DistributedAggregateWindowState<Integer>> finalAgg0All = windowMerger.triggerFinalWindow(triggerId20.get());
//        Assertions.assertEquals(finalAgg0All.size(), 1);
//        DistributedAggregateWindowState<Integer> finalAgg0 = finalAgg0All.get(0);
//        List<DistributedAggregateWindowState<Integer>> finalAgg1All = windowMerger.triggerFinalWindow(triggerId21.get());
//        Assertions.assertEquals(finalAgg1All.size(), 1);
//        DistributedAggregateWindowState<Integer> finalAgg1 = finalAgg1All.get(0);
//        Assertions.assertTrue(finalAgg0.hasValue());
//        Assertions.assertTrue(finalAgg1.hasValue());
//        Assertions.assertEquals(5, finalAgg0.getAggValues().get(0));
//        Assertions.assertEquals(10, finalAgg1.getAggValues().get(0));
        fail();
    }
}

