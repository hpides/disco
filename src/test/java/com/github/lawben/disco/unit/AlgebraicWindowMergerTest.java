package com.github.lawben.disco.unit;

import com.github.lawben.disco.AlgebraicWindowMerger;
import com.github.lawben.disco.aggregation.AlgebraicMergeFunction;
import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.PartialAverage;
import com.github.lawben.disco.utils.WindowMergerTestBase;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AlgebraicWindowMergerTest extends WindowMergerTestBase {
    @Test
    void testFinalTwoChildrenTwoWindowsAvg() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(new AlgebraicMergeFunction());
        int numChildren = 1;
        AlgebraicWindowMerger<PartialAverage> windowMerger = new AlgebraicWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1a = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2a = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3a = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        FunctionWindowAggregateId windowId1b = defaultFnWindowAggId(new WindowAggregateId(2,    0, 1000));
        FunctionWindowAggregateId windowId2b = defaultFnWindowAggId(new WindowAggregateId(2,  500, 1500));
        FunctionWindowAggregateId windowId3b = defaultFnWindowAggId(new WindowAggregateId(2, 1000, 2000));
        FunctionWindowAggregateId windowId4b = defaultFnWindowAggId(new WindowAggregateId(2, 1500, 2500));
        FunctionWindowAggregateId windowId5b = defaultFnWindowAggId(new WindowAggregateId(2, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId1a);
        windowMerger.processPreAggregate(new PartialAverage(3, 1), windowId1a);
        List<DistributedAggregateWindowState<PartialAverage>> final1aAll = windowMerger.triggerFinalWindow(windowId1a);
        Assertions.assertEquals(final1aAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final1a = final1aAll.get(0);
        Assertions.assertTrue(final1a.hasValue());
        Assertions.assertEquals(new PartialAverage(4, 2), final1a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(4, 1), windowId1b);
        windowMerger.processPreAggregate(new PartialAverage(6, 1), windowId1b);
        List<DistributedAggregateWindowState<PartialAverage>> final1bAll = windowMerger.triggerFinalWindow(windowId1b);
        Assertions.assertEquals(final1bAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final1b = final1bAll.get(0);
        Assertions.assertTrue(final1b.hasValue());
        Assertions.assertEquals(new PartialAverage(10, 2), final1b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(5, 1), windowId2b);
        windowMerger.processPreAggregate(new PartialAverage(7, 1), windowId2b);
        List<DistributedAggregateWindowState<PartialAverage>> final2bAll = windowMerger.triggerFinalWindow(windowId2b);
        Assertions.assertEquals(final2bAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final2b = final2bAll.get(0);
        Assertions.assertTrue(final2b.hasValue());
        Assertions.assertEquals(new PartialAverage(12, 2), final2b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 1), windowId2a);
        windowMerger.processPreAggregate(new PartialAverage(6, 2), windowId2a);
        List<DistributedAggregateWindowState<PartialAverage>> final2aAll = windowMerger.triggerFinalWindow(windowId2a);
        Assertions.assertEquals(final2aAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final2a = final2aAll.get(0);
        Assertions.assertTrue(final2a.hasValue());
        Assertions.assertEquals(new PartialAverage(8, 3), final2a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(6, 2), windowId3b);
        windowMerger.processPreAggregate(new PartialAverage(6, 3), windowId3b);
        List<DistributedAggregateWindowState<PartialAverage>> final3bAll = windowMerger.triggerFinalWindow(windowId3b);
        Assertions.assertEquals(final3bAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final3b = final3bAll.get(0);
        Assertions.assertTrue(final3b.hasValue());
        Assertions.assertEquals(new PartialAverage(12, 5), final3b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(7, 1), windowId4b);
        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId4b);
        List<DistributedAggregateWindowState<PartialAverage>> final4bAll = windowMerger.triggerFinalWindow(windowId4b);
        Assertions.assertEquals(final4bAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final4b = final4bAll.get(0);
        Assertions.assertTrue(final4b.hasValue());
        Assertions.assertEquals(new PartialAverage(8, 2), final4b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(  3, 1), windowId3a);
        windowMerger.processPreAggregate(new PartialAverage(101, 1), windowId3a);
        List<DistributedAggregateWindowState<PartialAverage>> final3aAll = windowMerger.triggerFinalWindow(windowId3a);
        Assertions.assertEquals(final3aAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final3a = final3aAll.get(0);
        Assertions.assertTrue(final3a.hasValue());
        Assertions.assertEquals(new PartialAverage(104, 2), final3a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(8, 1), windowId5b);
        windowMerger.processPreAggregate(new PartialAverage(0, 1), windowId5b);
        List<DistributedAggregateWindowState<PartialAverage>> final5bAll = windowMerger.triggerFinalWindow(windowId5b);
        Assertions.assertEquals(final5bAll.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final5b = final5bAll.get(0);
        Assertions.assertTrue(final5b.hasValue());
        Assertions.assertEquals(new PartialAverage(8, 2), final5b.getAggValues().get(0));
    }

    @Test
    void testFinalOneChildAvg() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new AlgebraicMergeFunction());
        int numChildren = 1;
        AlgebraicWindowMerger<PartialAverage> windowMerger = new AlgebraicWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId1);
        List<DistributedAggregateWindowState<PartialAverage>> final1All = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertEquals(final1All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final1 = final1All.get(0);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(new PartialAverage(1, 1), final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 1), windowId2);
        List<DistributedAggregateWindowState<PartialAverage>> final2All = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertEquals(final2All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final2 = final2All.get(0);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(new PartialAverage(2, 1), final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(3, 1), windowId3);
        List<DistributedAggregateWindowState<PartialAverage>> final3All = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertEquals(final3All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final3 = final3All.get(0);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(new PartialAverage(3, 1), final3.getAggValues().get(0));
    }

    @Test
    void testFinalTwoChildrenAvg() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new AlgebraicMergeFunction());
        int numChildren = 2;
        AlgebraicWindowMerger<PartialAverage> windowMerger = new AlgebraicWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage( 1, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(15, 3), windowId1);
        List<DistributedAggregateWindowState<PartialAverage>> final1All = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertEquals(final1All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final1 = final1All.get(0);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(new PartialAverage(16, 4), final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(6, 2), windowId2);
        List<DistributedAggregateWindowState<PartialAverage>> final2All = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertEquals(final2All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final2 = final2All.get(0);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(new PartialAverage(8, 4), final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(30, 3), windowId3);
        windowMerger.processPreAggregate(new PartialAverage( 6, 1), windowId3);
        List<DistributedAggregateWindowState<PartialAverage>> final3All = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertEquals(final3All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final3 = final3All.get(0);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(new PartialAverage(36, 4), final3.getAggValues().get(0));
    }


    @Test
    void testFinalFiveChildrenAvg() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(new AlgebraicMergeFunction());
        int numChildren = 5;
        AlgebraicWindowMerger<PartialAverage> windowMerger = new AlgebraicWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(2, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(3, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(4, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(5, 1), windowId1);
        List<DistributedAggregateWindowState<PartialAverage>> final1All = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertEquals(final1All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final1 = final1All.get(0);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(new PartialAverage(15, 5), final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(3, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(4, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(5, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(6, 2), windowId2);
        List<DistributedAggregateWindowState<PartialAverage>> final2All = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertEquals(final2All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final2 = final2All.get(0);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(new PartialAverage(20, 10), final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(30,  3), windowId3);
        windowMerger.processPreAggregate(new PartialAverage( 4,  1), windowId3);
        windowMerger.processPreAggregate(new PartialAverage(50,  2), windowId3);
        windowMerger.processPreAggregate(new PartialAverage( 6,  2), windowId3);
        windowMerger.processPreAggregate(new PartialAverage(70, 70), windowId3);
        List<DistributedAggregateWindowState<PartialAverage>> final3All = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertEquals(final3All.size(), 1);
        DistributedAggregateWindowState<PartialAverage> final3 = final3All.get(0);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(new PartialAverage(160, 78), final3.getAggValues().get(0));
    }
}
