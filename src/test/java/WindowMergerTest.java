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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WindowMergerTest {

    private AggregateFunction aggFn;
    private List<Window> windows;
    private DistributedWindowMerger<Integer> windowMerger;
    private Window tumblingWindow;

    @Before
    public void setup() {
        this.aggFn = DistributedUtils.aggregateFunctionSum();
        this.windows = new ArrayList<>();
        this.tumblingWindow = new TumblingWindow(WindowMeasure.Time, 1000, 1);
    }


    @Test
    public void testTriggerOneChild() {
        windows.add(tumblingWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        Optional<WindowAggregateId> triggerId1 = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId2 = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId3 = windowMerger.processPreAggregate(3, windowId3);

        Assert.assertTrue(triggerId1.isPresent());
        Assert.assertTrue(triggerId2.isPresent());
        Assert.assertTrue(triggerId3.isPresent());

        Assert.assertEquals(triggerId1.get(), windowId1);
        Assert.assertEquals(triggerId2.get(), windowId2);
        Assert.assertEquals(triggerId3.get(), windowId3);
    }

    @Test
    public void testTriggerTwoChildren() {
        windows.add(tumblingWindow);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        Optional<WindowAggregateId> triggerId1a = windowMerger.processPreAggregate(1, windowId1);
        Optional<WindowAggregateId> triggerId1b = windowMerger.processPreAggregate(1, windowId1);

        Optional<WindowAggregateId> triggerId2a = windowMerger.processPreAggregate(2, windowId2);
        Optional<WindowAggregateId> triggerId2b = windowMerger.processPreAggregate(2, windowId2);

        Optional<WindowAggregateId> triggerId3a = windowMerger.processPreAggregate(3, windowId3);
        Optional<WindowAggregateId> triggerId3b = windowMerger.processPreAggregate(3, windowId3);

        Assert.assertFalse(triggerId1a.isPresent());
        Assert.assertFalse(triggerId2a.isPresent());
        Assert.assertFalse(triggerId3a.isPresent());

        Assert.assertTrue(triggerId1b.isPresent());
        Assert.assertTrue(triggerId2b.isPresent());
        Assert.assertTrue(triggerId3b.isPresent());

        Assert.assertEquals(triggerId1b.get(), windowId1);
        Assert.assertEquals(triggerId2b.get(), windowId2);
        Assert.assertEquals(triggerId3b.get(), windowId3);
    }

    @Test
    public void testTriggerFiveChildren() {
        windows.add(tumblingWindow);
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

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

        Assert.assertFalse(triggerId1a.isPresent());
        Assert.assertFalse(triggerId2a.isPresent());
        Assert.assertFalse(triggerId3a.isPresent());
        Assert.assertFalse(triggerId1b.isPresent());
        Assert.assertFalse(triggerId2b.isPresent());
        Assert.assertFalse(triggerId3b.isPresent());
        Assert.assertFalse(triggerId1c.isPresent());
        Assert.assertFalse(triggerId2c.isPresent());
        Assert.assertFalse(triggerId3c.isPresent());
        Assert.assertFalse(triggerId1d.isPresent());
        Assert.assertFalse(triggerId2d.isPresent());
        Assert.assertFalse(triggerId3d.isPresent());

        Assert.assertTrue(triggerId1e.isPresent());
        Assert.assertTrue(triggerId2e.isPresent());
        Assert.assertTrue(triggerId3e.isPresent());

        Assert.assertEquals(triggerId1e.get(), windowId1);
        Assert.assertEquals(triggerId2e.get(), windowId2);
        Assert.assertEquals(triggerId3e.get(), windowId3);
    }

    @Test
    public void testFinalOneChild() {
        windows.add(tumblingWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        windowMerger.processPreAggregate(1, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assert.assertTrue(final1.hasValue());
        Assert.assertEquals(1, final1.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(2, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assert.assertTrue(final2.hasValue());
        Assert.assertEquals(2, final2.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(3, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assert.assertTrue(final3.hasValue());
        Assert.assertEquals(3, final3.getAggValues().get(0).intValue());
    }

    @Test
    public void testFinalTwoChildren() {
        windows.add(tumblingWindow);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assert.assertTrue(final1.hasValue());
        Assert.assertEquals(3, final1.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assert.assertTrue(final2.hasValue());
        Assert.assertEquals(5, final2.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assert.assertTrue(final3.hasValue());
        Assert.assertEquals(7, final3.getAggValues().get(0).intValue());
    }

    @Test
    public void testFinalFiveChildren() {
        windows.add(tumblingWindow);
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        windowMerger.processPreAggregate(1, windowId1);
        windowMerger.processPreAggregate(2, windowId1);
        windowMerger.processPreAggregate(3, windowId1);
        windowMerger.processPreAggregate(4, windowId1);
        windowMerger.processPreAggregate(5, windowId1);
        AggregateWindow<Integer> final1 = windowMerger.triggerFinalWindow(windowId1);
        Assert.assertTrue(final1.hasValue());
        Assert.assertEquals(15, final1.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(2, windowId2);
        windowMerger.processPreAggregate(3, windowId2);
        windowMerger.processPreAggregate(4, windowId2);
        windowMerger.processPreAggregate(5, windowId2);
        windowMerger.processPreAggregate(6, windowId2);
        AggregateWindow<Integer> final2 = windowMerger.triggerFinalWindow(windowId2);
        Assert.assertTrue(final2.hasValue());
        Assert.assertEquals(20, final2.getAggValues().get(0).intValue());

        windowMerger.processPreAggregate(3, windowId3);
        windowMerger.processPreAggregate(4, windowId3);
        windowMerger.processPreAggregate(5, windowId3);
        windowMerger.processPreAggregate(6, windowId3);
        windowMerger.processPreAggregate(7, windowId3);
        AggregateWindow<Integer> final3 = windowMerger.triggerFinalWindow(windowId3);
        Assert.assertTrue(final3.hasValue());
        Assert.assertEquals(25, final3.getAggValues().get(0).intValue());
    }

    @Test
    public void testSessionOneChild() {
        SessionWindow sessionWindow = new SessionWindow(WindowMeasure.Time, 100, 1);
        windows.add(sessionWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,  10, 110);
        WindowAggregateId windowId2 = new WindowAggregateId(1,  20, 120);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 110, 210);
        WindowAggregateId windowId4 = new WindowAggregateId(1, 220, 320);

        Optional<WindowAggregateId> triggerId1 = windowMerger.processPreAggregate(5, windowId1);
        Optional<WindowAggregateId> triggerId2 = windowMerger.processPreAggregate(10, windowId2);
        Optional<WindowAggregateId> triggerId3 = windowMerger.processPreAggregate(15, windowId3);
        Optional<WindowAggregateId> triggerId4 = windowMerger.processPreAggregate(20, windowId4);

        Assert.assertFalse(triggerId1.isPresent());
        Assert.assertFalse(triggerId2.isPresent());
        Assert.assertFalse(triggerId3.isPresent());

        Assert.assertTrue(triggerId4.isPresent());
        WindowAggregateId expectedTriggerId = new WindowAggregateId(1, 10, 210);
        Assert.assertEquals(expectedTriggerId, triggerId4.get());

        AggregateWindow<Integer> finalAgg = windowMerger.triggerFinalWindow(triggerId4.get());
        Assert.assertTrue(finalAgg.hasValue());
        Assert.assertEquals(30, finalAgg.getAggValues().get(0).intValue());
    }
}
