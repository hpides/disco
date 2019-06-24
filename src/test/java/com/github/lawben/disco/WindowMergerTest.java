package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.AlgebraicPartial;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticPartial;
import com.github.lawben.disco.aggregation.PartialAverage;
import com.github.lawben.disco.aggregation.PartialMedian;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WindowMergerTest {

    private List<Window> windows;
    private Window tumblingWindow;
    private List<AggregateFunction> aggregateFunctions;
    private AggregateFunction sumFunction;
    private DistributedWindowMerger<Integer> windowMerger;

    @BeforeEach
    public void setup() {
        this.windows = new ArrayList<>();
        this.tumblingWindow = new TumblingWindow(WindowMeasure.Time, 1000, 1);
        this.aggregateFunctions = new ArrayList<>();
        this.sumFunction = DistributedUtils.aggregateFunctionSum();
    }

    FunctionWindowAggregateId defaultFnWindowAggId(WindowAggregateId windowAggregateId) {
        return new FunctionWindowAggregateId(windowAggregateId, 0);
    }

    @Test
    public void testTriggerOneChild() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        Optional<FunctionWindowAggregateId> triggerId1 = windowMerger.processPreAggregate(1, windowId1);
        Optional<FunctionWindowAggregateId> triggerId2 = windowMerger.processPreAggregate(2, windowId2);
        Optional<FunctionWindowAggregateId> triggerId3 = windowMerger.processPreAggregate(3, windowId3);

        Assertions.assertTrue(triggerId1.isPresent());
        Assertions.assertTrue(triggerId2.isPresent());
        Assertions.assertTrue(triggerId3.isPresent());

        Assertions.assertEquals(triggerId1.get(), windowId1);
        Assertions.assertEquals(triggerId2.get(), windowId2);
        Assertions.assertEquals(triggerId3.get(), windowId3);
    }

    @Test
    public void testTriggerOneChildTwoAggFns() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        FunctionWindowAggregateId functionWindowId10 = new FunctionWindowAggregateId(windowId1, 0);
        FunctionWindowAggregateId functionWindowId20 = new FunctionWindowAggregateId(windowId2, 0);
        FunctionWindowAggregateId functionWindowId30 = new FunctionWindowAggregateId(windowId3, 0);

        FunctionWindowAggregateId functionWindowId11 = new FunctionWindowAggregateId(windowId1, 1);
        FunctionWindowAggregateId functionWindowId21 = new FunctionWindowAggregateId(windowId2, 1);
        FunctionWindowAggregateId functionWindowId31 = new FunctionWindowAggregateId(windowId3, 1);

        Optional<FunctionWindowAggregateId> triggerId10 = windowMerger.processPreAggregate(1, functionWindowId10);
        Optional<FunctionWindowAggregateId> triggerId11 = windowMerger.processPreAggregate(1, functionWindowId11);

        Optional<FunctionWindowAggregateId> triggerId20 = windowMerger.processPreAggregate(2, functionWindowId20);
        Optional<FunctionWindowAggregateId> triggerId21 = windowMerger.processPreAggregate(2, functionWindowId21);

        Optional<FunctionWindowAggregateId> triggerId30 = windowMerger.processPreAggregate(3, functionWindowId30);
        Optional<FunctionWindowAggregateId> triggerId31 = windowMerger.processPreAggregate(3, functionWindowId31);

        Assertions.assertTrue(triggerId10.isPresent());
        Assertions.assertTrue(triggerId11.isPresent());
        Assertions.assertTrue(triggerId20.isPresent());
        Assertions.assertTrue(triggerId21.isPresent());
        Assertions.assertTrue(triggerId30.isPresent());
        Assertions.assertTrue(triggerId31.isPresent());

        Assertions.assertEquals(triggerId10.get(), functionWindowId10);
        Assertions.assertEquals(triggerId20.get(), functionWindowId20);
        Assertions.assertEquals(triggerId30.get(), functionWindowId30);

        Assertions.assertEquals(triggerId11.get(), functionWindowId11);
        Assertions.assertEquals(triggerId21.get(), functionWindowId21);
        Assertions.assertEquals(triggerId31.get(), functionWindowId31);
    }

    @Test
    public void testTriggerOneChildTwoWindows() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1a = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2a = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3a = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        FunctionWindowAggregateId windowId1b = defaultFnWindowAggId(new WindowAggregateId(2,    0, 1000));
        FunctionWindowAggregateId windowId2b = defaultFnWindowAggId(new WindowAggregateId(2,  500, 1500));
        FunctionWindowAggregateId windowId3b = defaultFnWindowAggId(new WindowAggregateId(2, 1000, 2000));
        FunctionWindowAggregateId windowId4b = defaultFnWindowAggId(new WindowAggregateId(2, 1500, 2500));
        FunctionWindowAggregateId windowId5b = defaultFnWindowAggId(new WindowAggregateId(2, 2000, 3000));

        Optional<FunctionWindowAggregateId> triggerId1a = windowMerger.processPreAggregate(1, windowId1a);
        Optional<FunctionWindowAggregateId> triggerId2a = windowMerger.processPreAggregate(2, windowId2a);
        Optional<FunctionWindowAggregateId> triggerId3a = windowMerger.processPreAggregate(3, windowId3a);

        Optional<FunctionWindowAggregateId> triggerId1b = windowMerger.processPreAggregate(4, windowId1b);
        Optional<FunctionWindowAggregateId> triggerId2b = windowMerger.processPreAggregate(5, windowId2b);
        Optional<FunctionWindowAggregateId> triggerId3b = windowMerger.processPreAggregate(6, windowId3b);
        Optional<FunctionWindowAggregateId> triggerId4b = windowMerger.processPreAggregate(7, windowId4b);
        Optional<FunctionWindowAggregateId> triggerId5b = windowMerger.processPreAggregate(8, windowId5b);

        Assertions.assertTrue(triggerId1a.isPresent());
        Assertions.assertTrue(triggerId2a.isPresent());
        Assertions.assertTrue(triggerId3a.isPresent());

        Assertions.assertTrue(triggerId1b.isPresent());
        Assertions.assertTrue(triggerId2b.isPresent());
        Assertions.assertTrue(triggerId3b.isPresent());
        Assertions.assertTrue(triggerId4b.isPresent());
        Assertions.assertTrue(triggerId5b.isPresent());

        Assertions.assertEquals(triggerId1a.get(), windowId1a);
        Assertions.assertEquals(triggerId2a.get(), windowId2a);
        Assertions.assertEquals(triggerId3a.get(), windowId3a);

        Assertions.assertEquals(triggerId1b.get(), windowId1b);
        Assertions.assertEquals(triggerId2b.get(), windowId2b);
        Assertions.assertEquals(triggerId3b.get(), windowId3b);
        Assertions.assertEquals(triggerId4b.get(), windowId4b);
        Assertions.assertEquals(triggerId5b.get(), windowId5b);
    }

    @Test
    public void testFinalTwoChildrenTwoWindows() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

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
        AggregateWindow final1a = windowMerger.triggerFinalWindow(windowId1a);
        Assertions.assertTrue(final1a.hasValue());
        Assertions.assertEquals(3, final1a.getAggValues().get(0));

        windowMerger.processPreAggregate(4, windowId1b);
        windowMerger.processPreAggregate(5, windowId1b);
        AggregateWindow final1b = windowMerger.triggerFinalWindow(windowId1b);
        Assertions.assertTrue(final1b.hasValue());
        Assertions.assertEquals(9, final1b.getAggValues().get(0));

        windowMerger.processPreAggregate(5, windowId2b);
        windowMerger.processPreAggregate(6, windowId2b);
        AggregateWindow final2b = windowMerger.triggerFinalWindow(windowId2b);
        Assertions.assertTrue(final2b.hasValue());
        Assertions.assertEquals(11, final2b.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2a);
        windowMerger.processPreAggregate(3, windowId2a);
        AggregateWindow final2a = windowMerger.triggerFinalWindow(windowId2a);
        Assertions.assertTrue(final2a.hasValue());
        Assertions.assertEquals(5, final2a.getAggValues().get(0));

        windowMerger.processPreAggregate(6, windowId3b);
        windowMerger.processPreAggregate(7, windowId3b);
        AggregateWindow final3b = windowMerger.triggerFinalWindow(windowId3b);
        Assertions.assertTrue(final3b.hasValue());
        Assertions.assertEquals(13, final3b.getAggValues().get(0));

        windowMerger.processPreAggregate(7, windowId4b);
        windowMerger.processPreAggregate(8, windowId4b);
        AggregateWindow final4b = windowMerger.triggerFinalWindow(windowId4b);
        Assertions.assertTrue(final4b.hasValue());
        Assertions.assertEquals(15, final4b.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3a);
        windowMerger.processPreAggregate(4, windowId3a);
        AggregateWindow final3a = windowMerger.triggerFinalWindow(windowId3a);
        Assertions.assertTrue(final3a.hasValue());
        Assertions.assertEquals(7, final3a.getAggValues().get(0));

        windowMerger.processPreAggregate(8, windowId5b);
        windowMerger.processPreAggregate(9, windowId5b);
        AggregateWindow final5b = windowMerger.triggerFinalWindow(windowId5b);
        Assertions.assertTrue(final5b.hasValue());
        Assertions.assertEquals(17, final5b.getAggValues().get(0));
    }

    @Test
    public void testFinalTwoChildrenTwoWindowsAvg() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(DistributedUtils.aggregateFunctionAverage());
        int numChildren = 1;
        DistributedWindowMerger<AlgebraicPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

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
        AggregateWindow final1a = windowMerger.triggerFinalWindow(windowId1a);
        Assertions.assertTrue(final1a.hasValue());
        Assertions.assertEquals(2, final1a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(4, 1), windowId1b);
        windowMerger.processPreAggregate(new PartialAverage(6, 1), windowId1b);
        AggregateWindow final1b = windowMerger.triggerFinalWindow(windowId1b);
        Assertions.assertTrue(final1b.hasValue());
        Assertions.assertEquals(5, final1b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(5, 1), windowId2b);
        windowMerger.processPreAggregate(new PartialAverage(7, 1), windowId2b);
        AggregateWindow final2b = windowMerger.triggerFinalWindow(windowId2b);
        Assertions.assertTrue(final2b.hasValue());
        Assertions.assertEquals(6, final2b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 1), windowId2a);
        windowMerger.processPreAggregate(new PartialAverage(6, 1), windowId2a);
        AggregateWindow final2a = windowMerger.triggerFinalWindow(windowId2a);
        Assertions.assertTrue(final2a.hasValue());
        Assertions.assertEquals(4, final2a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(6, 1), windowId3b);
        windowMerger.processPreAggregate(new PartialAverage(6, 1), windowId3b);
        AggregateWindow final3b = windowMerger.triggerFinalWindow(windowId3b);
        Assertions.assertTrue(final3b.hasValue());
        Assertions.assertEquals(6, final3b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(7, 1), windowId4b);
        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId4b);
        AggregateWindow final4b = windowMerger.triggerFinalWindow(windowId4b);
        Assertions.assertTrue(final4b.hasValue());
        Assertions.assertEquals(4, final4b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(  3, 1), windowId3a);
        windowMerger.processPreAggregate(new PartialAverage(101, 1), windowId3a);
        AggregateWindow final3a = windowMerger.triggerFinalWindow(windowId3a);
        Assertions.assertTrue(final3a.hasValue());
        Assertions.assertEquals(52, final3a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(8, 1), windowId5b);
        windowMerger.processPreAggregate(new PartialAverage(0, 1), windowId5b);
        AggregateWindow final5b = windowMerger.triggerFinalWindow(windowId5b);
        Assertions.assertTrue(final5b.hasValue());
        Assertions.assertEquals(4, final5b.getAggValues().get(0));
    }

    @Test
    public void testFinalTwoChildrenTwoWindowsMedian() {
        windows.add(tumblingWindow);
        windows.add(new SlidingWindow(WindowMeasure.Time, 1000, 500, 2));
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        int numChildren = 1;
        DistributedWindowMerger<HolisticPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1a = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2a = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3a = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        FunctionWindowAggregateId windowId1b = defaultFnWindowAggId(new WindowAggregateId(2,    0, 1000));
        FunctionWindowAggregateId windowId2b = defaultFnWindowAggId(new WindowAggregateId(2,  500, 1500));
        FunctionWindowAggregateId windowId3b = defaultFnWindowAggId(new WindowAggregateId(2, 1000, 2000));
        FunctionWindowAggregateId windowId4b = defaultFnWindowAggId(new WindowAggregateId(2, 1500, 2500));
        FunctionWindowAggregateId windowId5b = defaultFnWindowAggId(new WindowAggregateId(2, 2000, 3000));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(1, 1))), windowId1a);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(1, 1))), windowId1a);
        AggregateWindow final1a = windowMerger.triggerFinalWindow(windowId1a);
        Assertions.assertTrue(final1a.hasValue());
        Assertions.assertEquals(1, final1a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(4, 5))), windowId1b);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(6, 7))), windowId1b);
        AggregateWindow final1b = windowMerger.triggerFinalWindow(windowId1b);
        Assertions.assertTrue(final1b.hasValue());
        Assertions.assertEquals(6, final1b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(5, 1))), windowId2b);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(7, 2))), windowId2b);
        AggregateWindow final2b = windowMerger.triggerFinalWindow(windowId2b);
        Assertions.assertTrue(final2b.hasValue());
        Assertions.assertEquals(5, final2b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(2, 1))), windowId2a);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(6, 7, 5))), windowId2a);
        AggregateWindow final2a = windowMerger.triggerFinalWindow(windowId2a);
        Assertions.assertTrue(final2a.hasValue());
        Assertions.assertEquals(5, final2a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(6, 2))), windowId3b);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(6, 2))), windowId3b);
        AggregateWindow final3b = windowMerger.triggerFinalWindow(windowId3b);
        Assertions.assertTrue(final3b.hasValue());
        Assertions.assertEquals(6, final3b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(7, 1))), windowId4b);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(1, 1))), windowId4b);
        AggregateWindow final4b = windowMerger.triggerFinalWindow(windowId4b);
        Assertions.assertTrue(final4b.hasValue());
        Assertions.assertEquals(1, final4b.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(100, 2))), windowId3a);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(101, 1))), windowId3a);
        AggregateWindow final3a = windowMerger.triggerFinalWindow(windowId3a);
        Assertions.assertTrue(final3a.hasValue());
        Assertions.assertEquals(100, final3a.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(8, 1))), windowId5b);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<> (Arrays.asList(0, 1))), windowId5b);
        AggregateWindow final5b = windowMerger.triggerFinalWindow(windowId5b);
        Assertions.assertTrue(final5b.hasValue());
        Assertions.assertEquals(1, final5b.getAggValues().get(0));
    }

    @Test
    public void testTriggerTwoChildren() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        Optional<FunctionWindowAggregateId> triggerId1a = windowMerger.processPreAggregate(1, windowId1);
        Optional<FunctionWindowAggregateId> triggerId1b = windowMerger.processPreAggregate(1, windowId1);

        Optional<FunctionWindowAggregateId> triggerId2a = windowMerger.processPreAggregate(2, windowId2);
        Optional<FunctionWindowAggregateId> triggerId2b = windowMerger.processPreAggregate(2, windowId2);

        Optional<FunctionWindowAggregateId> triggerId3a = windowMerger.processPreAggregate(3, windowId3);
        Optional<FunctionWindowAggregateId> triggerId3b = windowMerger.processPreAggregate(3, windowId3);

        Assertions.assertFalse(triggerId1a.isPresent());
        Assertions.assertFalse(triggerId2a.isPresent());
        Assertions.assertFalse(triggerId3a.isPresent());

        Assertions.assertTrue(triggerId1b.isPresent());
        Assertions.assertTrue(triggerId2b.isPresent());
        Assertions.assertTrue(triggerId3b.isPresent());

        Assertions.assertEquals(triggerId1b.get(), windowId1);
        Assertions.assertEquals(triggerId2b.get(), windowId2);
        Assertions.assertEquals(triggerId3b.get(), windowId3);
    }

    @Test
    public void testTriggerFiveChildren() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        Optional<FunctionWindowAggregateId> triggerId1a = windowMerger.processPreAggregate(1, windowId1);
        Optional<FunctionWindowAggregateId> triggerId1b = windowMerger.processPreAggregate(1, windowId1);
        Optional<FunctionWindowAggregateId> triggerId1c = windowMerger.processPreAggregate(1, windowId1);
        Optional<FunctionWindowAggregateId> triggerId1d = windowMerger.processPreAggregate(1, windowId1);
        Optional<FunctionWindowAggregateId> triggerId1e = windowMerger.processPreAggregate(1, windowId1);

        Optional<FunctionWindowAggregateId> triggerId2a = windowMerger.processPreAggregate(2, windowId2);
        Optional<FunctionWindowAggregateId> triggerId2b = windowMerger.processPreAggregate(2, windowId2);
        Optional<FunctionWindowAggregateId> triggerId2c = windowMerger.processPreAggregate(2, windowId2);
        Optional<FunctionWindowAggregateId> triggerId2d = windowMerger.processPreAggregate(2, windowId2);
        Optional<FunctionWindowAggregateId> triggerId2e = windowMerger.processPreAggregate(2, windowId2);

        Optional<FunctionWindowAggregateId> triggerId3a = windowMerger.processPreAggregate(3, windowId3);
        Optional<FunctionWindowAggregateId> triggerId3b = windowMerger.processPreAggregate(3, windowId3);
        Optional<FunctionWindowAggregateId> triggerId3c = windowMerger.processPreAggregate(3, windowId3);
        Optional<FunctionWindowAggregateId> triggerId3d = windowMerger.processPreAggregate(3, windowId3);
        Optional<FunctionWindowAggregateId> triggerId3e = windowMerger.processPreAggregate(3, windowId3);

        Assertions.assertFalse(triggerId1a.isPresent());
        Assertions.assertFalse(triggerId2a.isPresent());
        Assertions.assertFalse(triggerId3a.isPresent());
        Assertions.assertFalse(triggerId1b.isPresent());
        Assertions.assertFalse(triggerId2b.isPresent());
        Assertions.assertFalse(triggerId3b.isPresent());
        Assertions.assertFalse(triggerId1c.isPresent());
        Assertions.assertFalse(triggerId2c.isPresent());
        Assertions.assertFalse(triggerId3c.isPresent());
        Assertions.assertFalse(triggerId1d.isPresent());
        Assertions.assertFalse(triggerId2d.isPresent());
        Assertions.assertFalse(triggerId3d.isPresent());

        Assertions.assertTrue(triggerId1e.isPresent());
        Assertions.assertTrue(triggerId2e.isPresent());
        Assertions.assertTrue(triggerId3e.isPresent());

        Assertions.assertEquals(triggerId1e.get(), windowId1);
        Assertions.assertEquals(triggerId2e.get(), windowId2);
        Assertions.assertEquals(triggerId3e.get(), windowId3);
    }

    @Test
    public void testFinalOneChild() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(3, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalOneChildAvg() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionAverage());
        int numChildren = 1;
        DistributedWindowMerger<AlgebraicPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId1);
        AggregateWindow final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 1), windowId2);
        AggregateWindow final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(3, 1), windowId3);
        AggregateWindow final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(3, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalOneChildMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        int numChildren = 1;
        DistributedWindowMerger<HolisticPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1))), windowId1);
        AggregateWindow final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        AggregateWindow final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 3, 5, 7, 9, 11))), windowId3);
        AggregateWindow final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(7, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalTwoChildren() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(3, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(5, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(7, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalTwoChildrenAvg() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionAverage());
        int numChildren = 2;
        DistributedWindowMerger<AlgebraicPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage( 1, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(15, 3), windowId1);
        AggregateWindow final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(4, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(6, 2), windowId2);
        AggregateWindow final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(30, 3), windowId3);
        windowMerger.processPreAggregate(new PartialAverage( 6, 1), windowId3);
        AggregateWindow final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(9, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalTwoChildrenMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        int numChildren = 2;
        DistributedWindowMerger<HolisticPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2))), windowId1);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(2, 3))), windowId1);
        AggregateWindow final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(2, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        AggregateWindow final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(10, 20, 30, 40, 50))), windowId3);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(100, 200, 300, 400))), windowId3);
        AggregateWindow final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(50, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalFiveChildren() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        windowMerger.processPreAggregate(3, windowId1);
        windowMerger.processPreAggregate(4, windowId1);
        windowMerger.processPreAggregate(5, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(15, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        windowMerger.processPreAggregate(4, windowId2);
        windowMerger.processPreAggregate(5, windowId2);
        windowMerger.processPreAggregate(6, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(20, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        windowMerger.processPreAggregate(5, windowId3);
        windowMerger.processPreAggregate(6, windowId3);
        windowMerger.processPreAggregate(7, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(25, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalFiveChildrenAvg() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionAverage());
        int numChildren = 5;
        DistributedWindowMerger<AlgebraicPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialAverage(1, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(2, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(3, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(4, 1), windowId1);
        windowMerger.processPreAggregate(new PartialAverage(5, 1), windowId1);
        AggregateWindow final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(3, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(2, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(3, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(4, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(5, 2), windowId2);
        windowMerger.processPreAggregate(new PartialAverage(6, 2), windowId2);
        AggregateWindow final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialAverage(30,  3), windowId3);
        windowMerger.processPreAggregate(new PartialAverage( 4,  1), windowId3);
        windowMerger.processPreAggregate(new PartialAverage(50,  2), windowId3);
        windowMerger.processPreAggregate(new PartialAverage( 6,  2), windowId3);
        windowMerger.processPreAggregate(new PartialAverage(70, 70), windowId3);
        AggregateWindow final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(2, final3.getAggValues().get(0));
    }

    @Test
    public void testFinalFiveChildrenMedian() {
        windows.add(tumblingWindow);
        aggregateFunctions.add(DistributedUtils.aggregateFunctionMedian());
        int numChildren = 5;
        DistributedWindowMerger<HolisticPartial> windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,    0, 1000));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 1000, 2000));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 2000, 3000));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2))), windowId1);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(2, 3))), windowId1);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(3, 4))), windowId1);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(4, 5))), windowId1);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(5, 6))), windowId1);
        AggregateWindow final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(4, final1.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1, 2, 3))), windowId2);
        AggregateWindow final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0));

        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1))), windowId3);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1))), windowId3);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1))), windowId3);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(1))), windowId3);
        windowMerger.processPreAggregate(new PartialMedian(new ArrayList<>(Arrays.asList(2))), windowId3);
        AggregateWindow final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(1, final3.getAggValues().get(0));
    }

    @Test
    public void testSessionOneChild() {
        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
        windows.add(sessionWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,  10, 110));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1, 120, 320));

        Optional<FunctionWindowAggregateId> triggerId1 = windowMerger.processPreAggregate(5, windowId1);
        Optional<FunctionWindowAggregateId> triggerId2 = windowMerger.processPreAggregate(20, windowId2);

        Assertions.assertFalse(triggerId1.isPresent());
        Assertions.assertTrue(triggerId2.isPresent());

        FunctionWindowAggregateId expectedTriggerId = defaultFnWindowAggId(new WindowAggregateId(1, 10, 110));
        Assertions.assertEquals(expectedTriggerId, triggerId2.get());

        AggregateWindow<Integer> finalAgg = windowMerger.triggerFinalWindow(triggerId2.get());
        Assertions.assertTrue(finalAgg.hasValue());
        Assertions.assertEquals(5, finalAgg.getAggValues().get(0));
    }

    @Test
    public void testSessionFourChildren() {
        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
        windows.add(sessionWindow);
        aggregateFunctions.add(sumFunction);
        int numChildren = 4;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        FunctionWindowAggregateId windowId1 = defaultFnWindowAggId(new WindowAggregateId(1,  10, 110));
        FunctionWindowAggregateId windowId2 = defaultFnWindowAggId(new WindowAggregateId(1,  20, 120));
        FunctionWindowAggregateId windowId3 = defaultFnWindowAggId(new WindowAggregateId(1, 110, 210));
        FunctionWindowAggregateId windowId4 = defaultFnWindowAggId(new WindowAggregateId(1, 220, 320));

        Optional<FunctionWindowAggregateId> triggerId1 = windowMerger.processPreAggregate(5, windowId1);
        Optional<FunctionWindowAggregateId> triggerId2 = windowMerger.processPreAggregate(10, windowId2);
        Optional<FunctionWindowAggregateId> triggerId3 = windowMerger.processPreAggregate(15, windowId3);
        Optional<FunctionWindowAggregateId> triggerId4 = windowMerger.processPreAggregate(20, windowId4);

        Assertions.assertFalse(triggerId1.isPresent());
        Assertions.assertFalse(triggerId2.isPresent());
        Assertions.assertFalse(triggerId3.isPresent());

        Assertions.assertTrue(triggerId4.isPresent());
        FunctionWindowAggregateId expectedTriggerId = defaultFnWindowAggId(new WindowAggregateId(1, 10, 210));
        Assertions.assertEquals(expectedTriggerId, triggerId4.get());

        AggregateWindow<Integer> finalAgg = windowMerger.triggerFinalWindow(triggerId4.get());
        Assertions.assertTrue(finalAgg.hasValue());
        Assertions.assertEquals(30, finalAgg.getAggValues().get(0));
    }

    @Test
    public void testSessionOneChildTwoAggFns() {
        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
        windows.add(sessionWindow);
        aggregateFunctions.add(sumFunction);
        aggregateFunctions.add(sumFunction);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggregateFunctions);

        WindowAggregateId windowId1 = new WindowAggregateId(1,  10, 110);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 120, 320);

        FunctionWindowAggregateId functionWindowId10 = new FunctionWindowAggregateId(windowId1, 0);
        FunctionWindowAggregateId functionWindowId20 = new FunctionWindowAggregateId(windowId2, 0);

        FunctionWindowAggregateId functionWindowId11 = new FunctionWindowAggregateId(windowId1, 1);
        FunctionWindowAggregateId functionWindowId21 = new FunctionWindowAggregateId(windowId2, 1);

        Optional<FunctionWindowAggregateId> triggerId10 = windowMerger.processPreAggregate(5, functionWindowId10);
        Optional<FunctionWindowAggregateId> triggerId11 = windowMerger.processPreAggregate(10, functionWindowId11);

        Optional<FunctionWindowAggregateId> triggerId20 = windowMerger.processPreAggregate(15, functionWindowId20);
        Optional<FunctionWindowAggregateId> triggerId21 = windowMerger.processPreAggregate(20, functionWindowId21);

        Assertions.assertFalse(triggerId10.isPresent());
        Assertions.assertFalse(triggerId11.isPresent());

        Assertions.assertTrue(triggerId20.isPresent());
        Assertions.assertTrue(triggerId21.isPresent());

        FunctionWindowAggregateId expectedTriggerId0 = new FunctionWindowAggregateId(new WindowAggregateId(1, 10, 110), 0);
        FunctionWindowAggregateId expectedTriggerId1 = new FunctionWindowAggregateId(new WindowAggregateId(1, 10, 110), 1);
        Assertions.assertEquals(expectedTriggerId0, triggerId20.get());
        Assertions.assertEquals(expectedTriggerId1, triggerId21.get());

        AggregateWindow<Integer> finalAgg0 = windowMerger.triggerFinalWindow(triggerId20.get());
        AggregateWindow<Integer> finalAgg1 = windowMerger.triggerFinalWindow(triggerId21.get());
        Assertions.assertTrue(finalAgg0.hasValue());
        Assertions.assertTrue(finalAgg1.hasValue());
        Assertions.assertEquals(5, finalAgg0.getAggValues().get(0));
        Assertions.assertEquals(10, finalAgg1.getAggValues().get(0));
    }
}
