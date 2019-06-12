import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.DistributedWindowMerger;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WindowMergerTest {

    private List<AggregateFunction> aggFns;
    private List<Window> windows;
    private DistributedWindowMerger<Integer> windowMerger;
    private Window tumblingWindow;

    @BeforeEach
    public void setup() {
        this.aggFns = new ArrayList<>();
        this.aggFns.add(DistributedUtils.aggregateFunctionSum());
        this.windows = new ArrayList<>();
        this.tumblingWindow = new TumblingWindow(WindowMeasure.Time, 1000, 1);
    }

    @Test
    public void testTriggerOneChild() {
        windows.add(tumblingWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        Optional<WindowAggregateId> triggerId1 = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId2 = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId3 = windowMerger.processPreAggregate(3, windowId3);

        Assertions.assertTrue(triggerId1.isPresent());
        Assertions.assertTrue(triggerId2.isPresent());
        Assertions.assertTrue(triggerId3.isPresent());

        Assertions.assertEquals(triggerId1.get(), windowId1);
        Assertions.assertEquals(triggerId2.get(), windowId2);
        Assertions.assertEquals(triggerId3.get(), windowId3);
    }

    @Test
    public void testTriggerTwoChildren() {
        windows.add(tumblingWindow);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        Optional<WindowAggregateId> triggerId1a = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId1b = windowMerger.processPreAggregate(1, windowId1);

        Optional<WindowAggregateId> triggerId2a = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId2b = windowMerger.processPreAggregate(2, windowId2);

        Optional<WindowAggregateId> triggerId3a = windowMerger.processPreAggregate(3, windowId3);
        Optional<WindowAggregateId> triggerId3b = windowMerger.processPreAggregate(3, windowId3);

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
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        Optional<WindowAggregateId> triggerId1a = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId1b = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId1c = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId1d = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId1e = windowMerger.processPreAggregate(1, windowId1);

        Optional<WindowAggregateId> triggerId2a = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId2b = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId2c = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId2d = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId2e = windowMerger.processPreAggregate(2, windowId2);

        Optional<WindowAggregateId> triggerId3a = windowMerger.processPreAggregate(3, windowId3);
        Optional<WindowAggregateId> triggerId3b = windowMerger.processPreAggregate(3, windowId3);
        Optional<WindowAggregateId> triggerId3c = windowMerger.processPreAggregate(3, windowId3);
        Optional<WindowAggregateId> triggerId3d = windowMerger.processPreAggregate(3, windowId3);
        Optional<WindowAggregateId> triggerId3e = windowMerger.processPreAggregate(3, windowId3);

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
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        windowMerger.processPreAggregate(1, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(1, final1.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(2, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(2, final2.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(3, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(3, final3.getAggValues().get(0).intValue());
    }

    @Test
    public void testFinalTwoChildren() {
        windows.add(tumblingWindow);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(3, final1.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(5, final2.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(7, final3.getAggValues().get(0).intValue());
    }

    @Test
    public void testFinalFiveChildren() {
        windows.add(tumblingWindow);
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        windowMerger.processPreAggregate(3, windowId1);
        windowMerger.processPreAggregate(4, windowId1);
        windowMerger.processPreAggregate(5, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assertions.assertTrue(final1.hasValue());
        Assertions.assertEquals(15, final1.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        windowMerger.processPreAggregate(4, windowId2);
        windowMerger.processPreAggregate(5, windowId2);
        windowMerger.processPreAggregate(6, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assertions.assertTrue(final2.hasValue());
        Assertions.assertEquals(20, final2.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        windowMerger.processPreAggregate(5, windowId3);
        windowMerger.processPreAggregate(6, windowId3);
        windowMerger.processPreAggregate(7, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assertions.assertTrue(final3.hasValue());
        Assertions.assertEquals(25, final3.getAggValues().get(0).intValue());
    }

    @Test
    public void testSessionOneChild() {
        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
        windows.add(sessionWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFns);

        WindowAggregateId windowId1 = new WindowAggregateId(1,  10, 110);
        WindowAggregateId windowId2 = new WindowAggregateId(1,  20, 120);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 110, 210);
        WindowAggregateId windowId4 = new WindowAggregateId(1, 220, 320);

        Optional<WindowAggregateId> triggerId1 = windowMerger.processPreAggregate(5, windowId1);
        Optional<WindowAggregateId> triggerId2 = windowMerger.processPreAggregate(10, windowId2);
        Optional<WindowAggregateId> triggerId3 = windowMerger.processPreAggregate(15, windowId3);
        Optional<WindowAggregateId> triggerId4 = windowMerger.processPreAggregate(20, windowId4);

        Assertions.assertFalse(triggerId1.isPresent());
        Assertions.assertFalse(triggerId2.isPresent());
        Assertions.assertFalse(triggerId3.isPresent());

        Assertions.assertTrue(triggerId4.isPresent());
        WindowAggregateId expectedTriggerId = new WindowAggregateId(1, 10, 210);
        Assertions.assertEquals(expectedTriggerId, triggerId4.get());

        AggregateWindow<Integer> finalAgg = windowMerger.triggerFinalWindow(triggerId4.get());
        Assertions.assertTrue(finalAgg.hasValue());
        Assertions.assertEquals(30, finalAgg.getAggValues().get(0).intValue());
    }
}
