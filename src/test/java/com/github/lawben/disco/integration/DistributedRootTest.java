package com.github.lawben.disco.integration;

import static com.github.lawben.disco.DistributedUtils.ALGEBRAIC_STRING;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.DISTRIBUTIVE_STRING;
import static com.github.lawben.disco.DistributedUtils.HOLISTIC_STRING;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.DistributedUtils.WINDOW_COMPLETE;
import static com.github.lawben.disco.DistributedUtils.WINDOW_PARTIAL;
import static com.github.lawben.disco.DistributedUtils.functionWindowIdToString;
import static com.github.lawben.disco.DistributedUtils.slicesToString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.DistributedRoot;
import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.utils.BaseWindowMatcher;
import com.github.lawben.disco.utils.ZMQMock;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import com.github.lawben.disco.utils.ZMQRequestMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.slicing.slice.Slice;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.Utils;

public class DistributedRootTest {

    private int controllerPort;
    private int windowPort;
    private String resultPath;

    private List<ZMQPushMock> children;
    private ZMQPullMock resultListener;

    private Throwable threadException;
    private Thread.UncaughtExceptionHandler threadExceptionHandler = (th, ex) -> threadException = ex;

    @BeforeEach
    public void setup() throws IOException {
        this.controllerPort = Utils.findOpenPort();
        this.windowPort = Utils.findOpenPort();

        File tempFile = File.createTempFile("disco-test-", "");
        tempFile.deleteOnExit();
        this.resultPath = tempFile.getAbsolutePath();

        this.children = new ArrayList<>();
        this.resultListener = new ZMQPullMock(this.resultPath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        this.threadException = null;
        closeIfNotNull(this.resultListener);

        for (ZMQMock mock : this.children) {
            closeIfNotNull(mock);
        }
    }

    void closeIfNotNull(AutoCloseable x) throws Exception {
        if (x != null) {
            x.close();
        }
    }

    void runThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setUncaughtExceptionHandler(threadExceptionHandler);
        thread.start();
    }

    void registerChild() {
        ZMQRequestMock child = new ZMQRequestMock(controllerPort);
        child.addMessage("I am a new child");
        List<String> response = child.requestNext(3);
        assertNotNull(response.get(0));
        assertThat(response.get(1), anyOf(containsString("SESSION"), containsString("TUMBLING"), containsString("SLIDING")));
        assertThat(response.get(2), anyOf(containsString("SUM"), containsString("AVG"), containsString("MEDIAN")));

        children.add(new ZMQPushMock(windowPort));
    }

    DistributedRoot runDefaultRoot(int numChildren) throws Exception {
        List<String> windows = Collections.singletonList("TUMBLING,100,1");
        List<String> aggFns = Collections.singletonList("SUM");
        return runRoot(numChildren, windows, aggFns);
    }

    DistributedRoot runRoot(int numChildren, List<String> windows, List<String> aggFns) throws Exception {
        String windowsString = String.join(";", windows);
        String aggFnsString = String.join(";", aggFns);
        DistributedRoot root = new DistributedRoot(controllerPort, windowPort, resultPath, numChildren, windowsString, aggFnsString);
        runThread(root);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        for (int i = 0; i < numChildren; i++) {
            registerChild();
        }

        assertNull(threadException);
        return root;
    }

    FunctionWindowAggregateId getDefaultFunctionWindowId(WindowAggregateId windowId) {
        return new FunctionWindowAggregateId(windowId, 0);
    }

    void assertRootEnd() {
        String rootEnd = resultListener.receiveNext().get(0);
        assertThat(rootEnd, equalTo(STREAM_END));
    }

    void assertNoFinalThreadException(DistributedRoot root) throws InterruptedException {
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        root.interrupt();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);
    }

    void assertFunctionWindowIdStringEquals(String functionWindowString, FunctionWindowAggregateId functionWindowId) {
        assertTrue(BaseWindowMatcher.functionWindowIdStringsMatch(functionWindowId, functionWindowString));
    }

    @Test
    void testRegisterChild() throws Exception {
        int numChildren = 1;
        DistributedRoot root = runDefaultRoot(numChildren);
        assertNoFinalThreadException(root);
    }

    @Test
    void testRegisterTwoChildren() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runDefaultRoot(numChildren);
        assertNoFinalThreadException(root);
    }

    @Test
    void testRegisterFiveChildren() throws Exception {
        int numChildren = 5;
        DistributedRoot root = runDefaultRoot(numChildren);
        assertNoFinalThreadException(root);
    }

    @Test
    void testSingleChildSumAggregate() throws Exception {
        int numChildren = 1;
        DistributedRoot root = runDefaultRoot(numChildren);

        ZMQPushMock child = children.get(0);

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));
        child.addMessage("0", functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "5");
        child.addMessage("0", functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "7");
        child.addMessage(STREAM_END, "0");
        child.sendNext();
        child.sendNext();
        child.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("5"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("7"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testSingleChildAvgAggregate() throws Exception {
        int numChildren = 1;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Collections.singletonList("AVG"));

        ZMQPushMock child = children.get(0);

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));
        child.addMessage("0", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "6,2");
        child.addMessage("0", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "8,4");
        child.addMessage(STREAM_END, "0");
        child.sendNext();
        child.sendNext();
        child.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("3"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("2"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testSingleChildMedianAggregate() throws Exception {
        int numChildren = 1;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Collections.singletonList("MEDIAN"));

        ZMQPushMock child = children.get(0);

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        Slice slice1 = new DistributedSlice(  0, 100, Arrays.asList(1, 2, 3));
        Slice slice2 = new DistributedSlice(100, 150, Arrays.asList(5, 6, 7));
        Slice slice3 = new DistributedSlice(150, 200, Arrays.asList(0, 1, 2, 3, 4));

        child.addMessage("0", functionWindowIdToString(windowId1), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice1)));
        child.addMessage("0", functionWindowIdToString(windowId2), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice2, slice3)));
        child.addMessage(STREAM_END, "0");
        child.sendNext();
        child.sendNext();
        child.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("2"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("4"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenSumAggregate() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runDefaultRoot(numChildren);

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        String child1Id = "1";
        String child2Id = "2";

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        child1.addMessage(child1Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "5");
        child1.addMessage(child1Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "7");
        child1.addMessage(STREAM_END, child1Id);
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage(child2Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "3");
        child2.addMessage(child2Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "4");
        child2.addMessage(STREAM_END, child2Id);
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("8"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("11"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenSumAvgMedianAggregatesTwice() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Arrays.asList("SUM", "SUM", "AVG", "AVG", "MEDIAN", "MEDIAN"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        String child1Id = "1";
        String child2Id = "2";

        WindowAggregateId window1 = new WindowAggregateId(0,   0, 100);
        WindowAggregateId window2 = new WindowAggregateId(0, 100, 200);

        FunctionWindowAggregateId med1Window1Id = new FunctionWindowAggregateId(window1, 4);
        FunctionWindowAggregateId med2Window1Id = new FunctionWindowAggregateId(window1, 5);
        FunctionWindowAggregateId med1Window2Id = new FunctionWindowAggregateId(window2, 4);
        FunctionWindowAggregateId med2Window2Id = new FunctionWindowAggregateId(window2, 5);

        FunctionWindowAggregateId sum1Window1Id = new FunctionWindowAggregateId(window1, 0);
        FunctionWindowAggregateId sum2Window1Id = new FunctionWindowAggregateId(window1, 1);
        FunctionWindowAggregateId avg1Window1Id = new FunctionWindowAggregateId(window1, 2);
        FunctionWindowAggregateId avg2Window1Id = new FunctionWindowAggregateId(window1, 3);
        FunctionWindowAggregateId med11Window1Id = new FunctionWindowAggregateId(window1, 4, 1);
        FunctionWindowAggregateId med12Window1Id = new FunctionWindowAggregateId(window1, 5, 1);
        FunctionWindowAggregateId med21Window1Id = new FunctionWindowAggregateId(window1, 4, 2);
        FunctionWindowAggregateId med22Window1Id = new FunctionWindowAggregateId(window1, 5, 2);

        FunctionWindowAggregateId sum1Window2Id = new FunctionWindowAggregateId(window2, 0);
        FunctionWindowAggregateId sum2Window2Id = new FunctionWindowAggregateId(window2, 1);
        FunctionWindowAggregateId avg1Window2Id = new FunctionWindowAggregateId(window2, 2);
        FunctionWindowAggregateId avg2Window2Id = new FunctionWindowAggregateId(window2, 3);
        FunctionWindowAggregateId med11Window2Id = new FunctionWindowAggregateId(window2, 4, 1);
        FunctionWindowAggregateId med12Window2Id = new FunctionWindowAggregateId(window2, 5, 1);
        FunctionWindowAggregateId med21Window2Id = new FunctionWindowAggregateId(window2, 4, 2);
        FunctionWindowAggregateId med22Window2Id = new FunctionWindowAggregateId(window2, 5, 2);

        Slice slice1a = new DistributedSlice(  0,  50, Arrays.asList(1, 2, 3));
        Slice slice1b = new DistributedSlice( 50, 100, Arrays.asList(4, 5, 6));
        Slice slice1c = new DistributedSlice(100, 200, Arrays.asList(7, 8, 9));

        Slice slice2a = new DistributedSlice(  0,  50, Arrays.asList(7, 8, 9));
        Slice slice2b = new DistributedSlice( 50, 100, Arrays.asList(0, 9, 7));
        Slice slice2c = new DistributedSlice(100, 200, Arrays.asList(1, 2, 3));

        List<List<String>> child1Messages = Arrays.asList(
                // Window 1
                Arrays.asList(child1Id, functionWindowIdToString(sum1Window1Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "5"),
                Arrays.asList(child1Id, functionWindowIdToString(sum2Window1Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "2"),

                Arrays.asList(child1Id, functionWindowIdToString(avg1Window1Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "7,2"),
                Arrays.asList(child1Id, functionWindowIdToString(avg2Window1Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "100,4"),

                Arrays.asList(child1Id, functionWindowIdToString(med11Window1Id), HOLISTIC_STRING, WINDOW_PARTIAL,  slicesToString(Arrays.asList(slice1a))),
                Arrays.asList(child1Id, functionWindowIdToString(med11Window1Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice1b))),
                Arrays.asList(child1Id, functionWindowIdToString(med12Window1Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList())),

                // Window 2
                Arrays.asList(child1Id, functionWindowIdToString(sum1Window2Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "5"),
                Arrays.asList(child1Id, functionWindowIdToString(sum2Window2Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "9"),

                Arrays.asList(child1Id, functionWindowIdToString(avg1Window2Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "2,1"),
                Arrays.asList(child1Id, functionWindowIdToString(avg2Window2Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "0,2"),

                Arrays.asList(child1Id, functionWindowIdToString(med11Window2Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice1c))),
                Arrays.asList(child1Id, functionWindowIdToString(med12Window2Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList())),

                Arrays.asList(STREAM_END, child1Id)
        );

        for (List<String> msg : child1Messages) {
            child1.addMessage(msg);
            child1.sendNext();
        }

        List<List<String>> child2Messages = Arrays.asList(
                // Window 1
                Arrays.asList(child2Id, functionWindowIdToString(sum1Window1Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "10"),
                Arrays.asList(child2Id, functionWindowIdToString(sum2Window1Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "2"),

                Arrays.asList(child2Id, functionWindowIdToString(avg1Window1Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "10,3"),
                Arrays.asList(child2Id, functionWindowIdToString(avg2Window1Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "10,1"),

                Arrays.asList(child2Id, functionWindowIdToString(med21Window1Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice2a, slice2b))),
                Arrays.asList(child2Id, functionWindowIdToString(med22Window1Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList())),

                // Window 2
                Arrays.asList(child2Id, functionWindowIdToString(sum1Window2Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "8"),
                Arrays.asList(child2Id, functionWindowIdToString(sum2Window2Id), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "7"),

                Arrays.asList(child2Id, functionWindowIdToString(avg1Window2Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "5,5"),
                Arrays.asList(child2Id, functionWindowIdToString(avg2Window2Id), ALGEBRAIC_STRING, WINDOW_COMPLETE, "50,1"),

                Arrays.asList(child2Id, functionWindowIdToString(med21Window2Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice2c))),
                Arrays.asList(child2Id, functionWindowIdToString(med22Window2Id), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList())),

                Arrays.asList(STREAM_END, child2Id)
        );

        for (List<String> msg : child2Messages) {
            child2.addMessage(msg);
            child2.sendNext();
        }

        // Window 1, sum 1
        List<String> result1a = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1a.get(0), sum1Window1Id);
        assertThat(result1a.get(1), equalTo("15"));

        // Window 1, sum 2
        List<String> result1b = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1b.get(0), sum2Window1Id);
        assertThat(result1b.get(1), equalTo("4"));

        // Window 1, avg 1
        List<String> result1c = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1c.get(0), avg1Window1Id);
        assertThat(result1c.get(1), equalTo("3"));

        // Window 1, avg 2
        List<String> result1d = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1d.get(0), avg2Window1Id);
        assertThat(result1d.get(1), equalTo("22"));

        // Window 1, median 1
        List<String> result1e = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1e.get(0), med1Window1Id);
        assertThat(result1e.get(1), equalTo("6"));

        // Window 1, median 2
        List<String> result1f = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1f.get(0), med2Window1Id);
        assertThat(result1f.get(1), equalTo("6"));

        // Window 2, sum 1
        List<String> result2a = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2a.get(0), sum1Window2Id);
        assertThat(result2a.get(1), equalTo("13"));

        // Window 2, sum 2
        List<String> result2b = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2b.get(0), sum2Window2Id);
        assertThat(result2b.get(1), equalTo("16"));

        // Window 2, avg 1
        List<String> result2c = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2c.get(0), avg1Window2Id);
        assertThat(result2c.get(1), equalTo("1"));

        // Window 2, avg 2
        List<String> result2d = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2d.get(0), avg2Window2Id);
        assertThat(result2d.get(1), equalTo("16"));

        // Window 2, median 1
        List<String> result2e = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2e.get(0), med1Window2Id);
        assertThat(result2e.get(1), equalTo("7"));

        // Window 2, median 2
        List<String> result2f = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2f.get(0), med2Window2Id);
        assertThat(result2f.get(1), equalTo("7"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenAvgAggregate() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Collections.singletonList("AVG"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));
        child1.addMessage("0", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "6,2");
        child1.addMessage("0", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "14,4");
        child1.addMessage(STREAM_END, "0");
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage("1", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "9,3");
        child2.addMessage("1", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "10,2");
        child2.addMessage(STREAM_END, "1");
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("3"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("4"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenMedianAggregate() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("SLIDING,100,501"), Collections.singletonList("MEDIAN"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        FunctionWindowAggregateId window1 = getDefaultFunctionWindowId(new WindowAggregateId(0,   0, 100));
        FunctionWindowAggregateId window2 = getDefaultFunctionWindowId(new WindowAggregateId(0,  50, 150));
        FunctionWindowAggregateId window3 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        FunctionWindowAggregateId windowId11 = new FunctionWindowAggregateId(window1, 1);
        FunctionWindowAggregateId windowId21 = new FunctionWindowAggregateId(window2, 1);
        FunctionWindowAggregateId windowId31 = new FunctionWindowAggregateId(window3, 1);

        FunctionWindowAggregateId windowId12 = new FunctionWindowAggregateId(window1, 2);
        FunctionWindowAggregateId windowId22 = new FunctionWindowAggregateId(window2, 2);
        FunctionWindowAggregateId windowId32 = new FunctionWindowAggregateId(window3, 2);

        Slice slice1a = new DistributedSlice(  0,  50, Arrays.asList(1, 2, 3));
        Slice slice1b = new DistributedSlice( 50, 100, Arrays.asList(2, 3, 4));
        Slice slice1c = new DistributedSlice(100, 150, Arrays.asList(5, 6, 7));
        Slice slice1d = new DistributedSlice(150, 200, Arrays.asList(0, 1, 2, 3, 4));

        child1.addMessage("1", functionWindowIdToString(windowId11), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice1a, slice1b)));
        child1.addMessage("1", functionWindowIdToString(windowId21), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice1c)));
        child1.addMessage("1", functionWindowIdToString(windowId31), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice1d)));
        child1.addMessage(STREAM_END, "1");
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        Slice slice2a = new DistributedSlice(  0,  50, Arrays.asList(4, 5, 2, 3));
        Slice slice2b = new DistributedSlice( 50, 100, Arrays.asList(10, 20, 30));
        Slice slice2c = new DistributedSlice(100, 150, Arrays.asList(15, 25, 35));
        Slice slice2d = new DistributedSlice(150, 200, Collections.emptyList());

        child2.addMessage("2", functionWindowIdToString(windowId12), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice2a, slice2b)));
        child2.addMessage("2", functionWindowIdToString(windowId22), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice2c)));
        child2.addMessage("2", functionWindowIdToString(windowId32), HOLISTIC_STRING, WINDOW_COMPLETE, slicesToString(Arrays.asList(slice2d)));
        child2.addMessage(STREAM_END, "2");
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        List<String> result2 = resultListener.receiveNext(2);
        List<String> result3 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), window1);
        assertThat(result1.get(1), equalTo("3"));

        assertFunctionWindowIdStringEquals(result2.get(0), window2);
        assertThat(result2.get(1), equalTo("10"));

        assertFunctionWindowIdStringEquals(result3.get(0), window3);
        assertThat(result3.get(1), equalTo("5"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testFiveChildrenSumAggregate() throws Exception {
        int numChildren = 5;
        DistributedRoot root = runDefaultRoot(numChildren);

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);
        ZMQPushMock child3 = children.get(2);
        ZMQPushMock child4 = children.get(3);
        ZMQPushMock child5 = children.get(4);

        String child1Id = "1";
        String child2Id = "2";
        String child3Id = "3";
        String child4Id = "4";
        String child5Id = "5";

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        child1.addMessage(child1Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "5");
        child1.addMessage(child1Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "7");
        child1.addMessage(STREAM_END, child1Id);
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage(child2Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "3");
        child2.addMessage(child2Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "4");
        child2.addMessage(STREAM_END, child2Id);
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        child3.addMessage(child3Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "1");
        child3.addMessage(child3Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "0");
        child3.addMessage(STREAM_END, child3Id);
        child3.sendNext();
        child3.sendNext();
        child3.sendNext();

        child4.addMessage(child4Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "10");
        child4.addMessage(child4Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "100");
        child4.addMessage(STREAM_END, child4Id);
        child4.sendNext();
        child4.sendNext();
        child4.sendNext();

        child5.addMessage(child5Id, functionWindowIdToString(windowId1), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "0");
        child5.addMessage(child5Id, functionWindowIdToString(windowId2), DISTRIBUTIVE_STRING, WINDOW_COMPLETE, "3");
        child5.addMessage(STREAM_END, child5Id);
        child5.sendNext();
        child5.sendNext();
        child5.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("19"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("114"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testFiveChildrenAvgAggregate() throws Exception {
        int numChildren = 5;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Collections.singletonList("AVG"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);
        ZMQPushMock child3 = children.get(2);
        ZMQPushMock child4 = children.get(3);
        ZMQPushMock child5 = children.get(4);

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        child1.addMessage("1", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "6,2");
        child1.addMessage("1", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "14,4");
        child1.addMessage(STREAM_END, "1");
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage("2", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "9,3");
        child2.addMessage("2", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "10,2");
        child2.addMessage(STREAM_END, "2");
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        child3.addMessage("3", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "1,2");
        child3.addMessage("3", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "0,0");
        child3.addMessage(STREAM_END, "3");
        child3.sendNext();
        child3.sendNext();
        child3.sendNext();

        child4.addMessage("4", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "10,3");
        child4.addMessage("4", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "10,2");
        child4.addMessage(STREAM_END, "4");
        child4.sendNext();
        child4.sendNext();
        child4.sendNext();

        child5.addMessage("5", functionWindowIdToString(windowId1), ALGEBRAIC_STRING, WINDOW_COMPLETE, "150,2");
        child5.addMessage("5", functionWindowIdToString(windowId2), ALGEBRAIC_STRING, WINDOW_COMPLETE, "25,20");
        child5.addMessage(STREAM_END, "5");
        child5.sendNext();
        child5.sendNext();
        child5.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("14"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("2"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }
}
