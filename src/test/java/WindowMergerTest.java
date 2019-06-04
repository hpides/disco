import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.DistributedWindowMerger;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WindowMergerTest {

    private StateFactory stateFactory;
    private AggregateFunction aggFn;
    private List<Window> windows;
    private DistributedWindowMerger<Integer> windowMerger;
    private Window tumblingWindow;

    @Before
    public void setup() {
        this.stateFactory = new MemoryStateFactory();
        this.aggFn = DistributedUtils.aggregateFunctionSum();
        this.windows = new ArrayList<>();
        this.tumblingWindow = new TumblingWindow(WindowMeasure.Time, 1000);
    }


    @Test
    public void testTriggerOneChild() {
        windows.add(tumblingWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(stateFactory, numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        boolean trigger1 = windowMerger.processPreAggregate(1, windowId1);
        boolean trigger2 = windowMerger.processPreAggregate(2, windowId2);
        boolean trigger3 = windowMerger.processPreAggregate(3, windowId3);

        Assert.assertTrue(trigger1);
        Assert.assertTrue(trigger2);
        Assert.assertTrue(trigger3);
    }

    @Test
    public void testTriggerTwoChildren() {
        windows.add(tumblingWindow);
        int numChildren = 2;
        windowMerger = new DistributedWindowMerger<>(stateFactory, numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        boolean trigger1a = windowMerger.processPreAggregate(1, windowId1);
        boolean trigger1b = windowMerger.processPreAggregate(1, windowId1);

        boolean trigger2a = windowMerger.processPreAggregate(2, windowId2);
        boolean trigger2b = windowMerger.processPreAggregate(2, windowId2);

        boolean trigger3a = windowMerger.processPreAggregate(3, windowId3);
        boolean trigger3b = windowMerger.processPreAggregate(3, windowId3);

        Assert.assertFalse(trigger1a);
        Assert.assertFalse(trigger2a);
        Assert.assertFalse(trigger3a);

        Assert.assertTrue(trigger1b);
        Assert.assertTrue(trigger2b);
        Assert.assertTrue(trigger3b);
    }

    @Test
    public void testTriggerFiveChildren() {
        windows.add(tumblingWindow);
        int numChildren = 5;
        windowMerger = new DistributedWindowMerger<>(stateFactory, numChildren, windows, aggFn);

        WindowAggregateId windowId1 = new WindowAggregateId(1,    0, 1000);
        WindowAggregateId windowId2 = new WindowAggregateId(1, 1000, 2000);
        WindowAggregateId windowId3 = new WindowAggregateId(1, 2000, 3000);

        boolean trigger1a = windowMerger.processPreAggregate(1, windowId1);
        boolean trigger1b = windowMerger.processPreAggregate(1, windowId1);
        boolean trigger1c = windowMerger.processPreAggregate(1, windowId1);
        boolean trigger1d = windowMerger.processPreAggregate(1, windowId1);
        boolean trigger1e = windowMerger.processPreAggregate(1, windowId1);

        boolean trigger2a = windowMerger.processPreAggregate(2, windowId2);
        boolean trigger2b = windowMerger.processPreAggregate(2, windowId2);
        boolean trigger2c = windowMerger.processPreAggregate(2, windowId2);
        boolean trigger2d = windowMerger.processPreAggregate(2, windowId2);
        boolean trigger2e = windowMerger.processPreAggregate(2, windowId2);

        boolean trigger3a = windowMerger.processPreAggregate(3, windowId3);
        boolean trigger3b = windowMerger.processPreAggregate(3, windowId3);
        boolean trigger3c = windowMerger.processPreAggregate(3, windowId3);
        boolean trigger3d = windowMerger.processPreAggregate(3, windowId3);
        boolean trigger3e = windowMerger.processPreAggregate(3, windowId3);

        Assert.assertFalse(trigger1a);
        Assert.assertFalse(trigger2a);
        Assert.assertFalse(trigger3a);
        Assert.assertFalse(trigger1b);
        Assert.assertFalse(trigger2b);
        Assert.assertFalse(trigger3b);
        Assert.assertFalse(trigger1c);
        Assert.assertFalse(trigger2c);
        Assert.assertFalse(trigger3c);
        Assert.assertFalse(trigger1d);
        Assert.assertFalse(trigger2d);
        Assert.assertFalse(trigger3d);

        Assert.assertTrue(trigger1e);
        Assert.assertTrue(trigger2e);
        Assert.assertTrue(trigger3e);
    }

    @Test
    public void testFinalOneChild() {
        windows.add(tumblingWindow);
        int numChildren = 1;
        windowMerger = new DistributedWindowMerger<>(stateFactory, numChildren, windows, aggFn);

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
        windowMerger = new DistributedWindowMerger<>(stateFactory, numChildren, windows, aggFn);

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
        windowMerger = new DistributedWindowMerger<>(stateFactory, numChildren, windows, aggFn);

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
}
