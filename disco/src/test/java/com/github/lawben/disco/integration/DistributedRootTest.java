package com.github.lawben.disco.integration;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.EVENT_STRING;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.DistributedUtils.functionWindowIdToString;
import static com.github.lawben.disco.aggregation.FunctionWindowAggregateId.NO_CHILD_ID;
import static com.github.lawben.disco.utils.TestUtils.closeIfNotNull;
import static com.github.lawben.disco.utils.TestUtils.receiveResultWindows;
import static com.github.lawben.disco.utils.TestUtils.runThread;
import static com.github.lawben.disco.utils.WindowResultMatcher.equalsWindowResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.DistributedRoot;
import com.github.lawben.disco.aggregation.AlgebraicWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.DistributiveWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticWindowAggregate;
import com.github.lawben.disco.aggregation.PartialAverage;
import com.github.lawben.disco.utils.WindowMatcher;
import com.github.lawben.disco.utils.ZMQMock;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import com.github.lawben.disco.utils.ZMQRequestMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hamcrest.Matcher;
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

    void registerChild(int childId) {
        ZMQRequestMock child = new ZMQRequestMock(controllerPort);
        child.addMessage("[CHILD-" + childId + "] I am a new child");
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
            registerChild(i);
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
        assertTrue(WindowMatcher.functionWindowIdStringsMatch(functionWindowId, functionWindowString));
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
        String windowAggregate1 = new DistributiveWindowAggregate(5).asString();
        String windowAggregate2 = new DistributiveWindowAggregate(7).asString();
        child.sendNext("0", functionWindowIdToString(windowId1), "1", windowAggregate1);
        child.sendNext("0", functionWindowIdToString(windowId2), "1", windowAggregate2);
        child.sendNext(STREAM_END, "0");

        List<String> result1 = resultListener.receiveNext(2);
        assertThat(result1, equalsWindowResult(windowId1, 5L));

        List<String> result2 = resultListener.receiveNext(2);
        assertThat(result2, equalsWindowResult(windowId2, 7L));

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
        String windowAggregate1 = new AlgebraicWindowAggregate(new PartialAverage(6, 2)).asString();
        String windowAggregate2 = new AlgebraicWindowAggregate(new PartialAverage(8, 4)).asString();
        child.sendNext("0", functionWindowIdToString(windowId1), "1", windowAggregate1);
        child.sendNext("0", functionWindowIdToString(windowId2), "1", windowAggregate2);
        child.sendNext(STREAM_END, "0");

        List<String> result1 = resultListener.receiveNext(2);
        assertThat(result1, equalsWindowResult(windowId1, 3L));

        List<String> result2 = resultListener.receiveNext(2);
        assertThat(result2, equalsWindowResult(windowId2, 2L));

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

        DistributedSlice slice1 = new DistributedSlice(  0, 100, 1, 2, 3);
        DistributedSlice slice2 = new DistributedSlice(100, 150, 5, 6, 7);
        DistributedSlice slice3 = new DistributedSlice(150, 200, 0, 1, 2, 3, 4);

        String windowAggregate1 = new HolisticWindowAggregate(Arrays.asList(slice1)).asString();
        String windowAggregate2 = new HolisticWindowAggregate(Arrays.asList(slice2, slice3)).asString();
        child.sendNext("0", functionWindowIdToString(windowId1), "1", windowAggregate1);
        child.sendNext("0", functionWindowIdToString(windowId2), "1", windowAggregate2);
        child.sendNext(STREAM_END, "0");

        List<String> result1 = resultListener.receiveNext(2);
        assertThat(result1, equalsWindowResult(windowId1, 2L));

        List<String> result2 = resultListener.receiveNext(2);
        assertThat(result2, equalsWindowResult(windowId2, 4L));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenSumAggregate() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runDefaultRoot(numChildren);

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        String child1Id = "0";
        String child2Id = "1";

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        String windowAggregate1 = new DistributiveWindowAggregate(5).asString();
        String windowAggregate2 = new DistributiveWindowAggregate(7).asString();
        child1.sendNext(child1Id, functionWindowIdToString(windowId1), "1", windowAggregate1);
        child1.sendNext(child1Id, functionWindowIdToString(windowId2), "1", windowAggregate2);
        child1.sendNext(STREAM_END, child1Id);

        String windowAggregate3 = new DistributiveWindowAggregate(3).asString();
        String windowAggregate4 = new DistributiveWindowAggregate(4).asString();
        child2.sendNext(child2Id, functionWindowIdToString(windowId1), "1", windowAggregate3);
        child2.sendNext(child2Id, functionWindowIdToString(windowId2), "1", windowAggregate4);
        child2.sendNext(STREAM_END, child2Id);

        List<String> result1 = resultListener.receiveNext(2);
        assertThat(result1, equalsWindowResult(windowId1, 8L));

        List<String> result2 = resultListener.receiveNext(2);
        assertThat(result2, equalsWindowResult(windowId2, 11L));

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

        String child1Id = "0";
        String child2Id = "1";

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

        DistributedSlice slice1a = new DistributedSlice(  0,  50, 1, 2, 3);
        DistributedSlice slice1b = new DistributedSlice( 50, 100, 4, 5, 6);
        DistributedSlice slice1c = new DistributedSlice(100, 200, 7, 8, 9);

        DistributedSlice slice2a = new DistributedSlice(  0,  50, 7, 8, 9);
        DistributedSlice slice2b = new DistributedSlice( 50, 100, 0, 9, 7);
        DistributedSlice slice2c = new DistributedSlice(100, 200, 1, 2, 3);

        List<List<String>> child1Messages = Arrays.asList(
                // Window 1
                Arrays.asList(child1Id, functionWindowIdToString(sum1Window1Id), "1", new DistributiveWindowAggregate(5).asString()),
                Arrays.asList(child1Id, functionWindowIdToString(sum2Window1Id), "1", new DistributiveWindowAggregate(2).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(avg1Window1Id), "1", new AlgebraicWindowAggregate(new PartialAverage(7, 2)).asString()),
                Arrays.asList(child1Id, functionWindowIdToString(avg2Window1Id), "1", new AlgebraicWindowAggregate(new PartialAverage(100, 4)).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(med11Window1Id), "1", new HolisticWindowAggregate(Arrays.asList(slice1a, slice1b)).asString()),
                Arrays.asList(child1Id, functionWindowIdToString(med12Window1Id), "1", new HolisticWindowAggregate(Arrays.asList()).asString()),

                // Window 2
                Arrays.asList(child1Id, functionWindowIdToString(sum1Window2Id), "1", new DistributiveWindowAggregate(5).asString()),
                Arrays.asList(child1Id, functionWindowIdToString(sum2Window2Id), "1", new DistributiveWindowAggregate(9).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(avg1Window2Id), "1", new AlgebraicWindowAggregate(new PartialAverage(2, 1)).asString()),
                Arrays.asList(child1Id, functionWindowIdToString(avg2Window2Id), "1", new AlgebraicWindowAggregate(new PartialAverage(0, 2)).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(med11Window2Id), "1", new HolisticWindowAggregate(Arrays.asList(slice1c)).asString()),
                Arrays.asList(child1Id, functionWindowIdToString(med12Window2Id), "1", new HolisticWindowAggregate(Arrays.asList()).asString()),

                Arrays.asList(STREAM_END, child1Id)
        );

        for (List<String> msg : child1Messages) {
            child1.addMessage(msg);
            child1.sendNext();
        }

        List<List<String>> child2Messages = Arrays.asList(
                // Window 1
                Arrays.asList(child2Id, functionWindowIdToString(sum1Window1Id), "1", new DistributiveWindowAggregate(10).asString()),
                Arrays.asList(child2Id, functionWindowIdToString(sum2Window1Id), "1", new DistributiveWindowAggregate(2).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(avg1Window1Id), "1", new AlgebraicWindowAggregate(new PartialAverage(10, 3)).asString()),
                Arrays.asList(child2Id, functionWindowIdToString(avg2Window1Id), "1", new AlgebraicWindowAggregate(new PartialAverage(10, 1)).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(med21Window1Id), "1", new HolisticWindowAggregate(Arrays.asList(slice2a, slice2b)).asString()),
                Arrays.asList(child2Id, functionWindowIdToString(med22Window1Id), "1", new HolisticWindowAggregate(Arrays.asList()).asString()),

                // Window 2
                Arrays.asList(child2Id, functionWindowIdToString(sum1Window2Id), "1", new DistributiveWindowAggregate(8).asString()),
                Arrays.asList(child2Id, functionWindowIdToString(sum2Window2Id), "1", new DistributiveWindowAggregate(7).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(avg1Window2Id), "1", new AlgebraicWindowAggregate(new PartialAverage(5, 5)).asString()),
                Arrays.asList(child2Id, functionWindowIdToString(avg2Window2Id), "1", new AlgebraicWindowAggregate(new PartialAverage(50, 1)).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(med21Window2Id), "1", new HolisticWindowAggregate(Arrays.asList(slice2c)).asString()),
                Arrays.asList(child2Id, functionWindowIdToString(med22Window2Id), "1", new HolisticWindowAggregate(Arrays.asList()).asString()),

                Arrays.asList(STREAM_END, child2Id)
        );

        for (List<String> msg : child2Messages) {
            child2.addMessage(msg);
            child2.sendNext();
        }

        List<Matcher<? super List<String>>> windowResultMatchers = Arrays.asList(
                equalsWindowResult(sum1Window1Id, 15L),
                equalsWindowResult(sum2Window1Id,  4L),
                equalsWindowResult(avg1Window1Id,  3L),
                equalsWindowResult(avg2Window1Id, 22L),
                equalsWindowResult(med1Window1Id,  6L),
                equalsWindowResult(med2Window1Id,  6L),
                equalsWindowResult(sum1Window2Id, 13L),
                equalsWindowResult(sum2Window2Id, 16L),
                equalsWindowResult(avg1Window2Id,  1L),
                equalsWindowResult(avg2Window2Id, 16L),
                equalsWindowResult(med1Window2Id,  7L),
                equalsWindowResult(med2Window2Id,  7L)
        );

        List<List<String>> windowStrings = new ArrayList<>(windowResultMatchers.size());
        for (int i = 0; i < windowResultMatchers.size(); i++) {
            windowStrings.add(resultListener.receiveNext(2));
        }

        assertThat(windowStrings, containsInAnyOrder(windowResultMatchers));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

//    @Test
    void testTwoChildrenAvgAggregate() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Collections.singletonList("AVG"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));
        String windowAggregate1 = new AlgebraicWindowAggregate(new PartialAverage(6, 2)).asString();
        String windowAggregate2 = new AlgebraicWindowAggregate(new PartialAverage(14, 4)).asString();
        child1.sendNext("0", functionWindowIdToString(windowId1), "1", windowAggregate1);
        child1.sendNext("0", functionWindowIdToString(windowId2), "1", windowAggregate2);
        child1.sendNext(STREAM_END, "0");

        String windowAggregate3 = new AlgebraicWindowAggregate(new PartialAverage(9, 3)).asString();
        String windowAggregate4 = new AlgebraicWindowAggregate(new PartialAverage(10, 2)).asString();
        child2.sendNext("1", functionWindowIdToString(windowId1), "1", windowAggregate3);
        child2.sendNext("1", functionWindowIdToString(windowId2), "1", windowAggregate4);
        child2.sendNext(STREAM_END, "1");

        List<String> result1 = resultListener.receiveNext(2);
        assertThat(result1, equalsWindowResult(windowId1, 3L));

        List<String> result2 = resultListener.receiveNext(2);
        assertThat(result2, equalsWindowResult(windowId2, 4L));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenMedianAggregate() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("SLIDING,100,50"), Collections.singletonList("MEDIAN"));

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

        DistributedSlice slice1a = new DistributedSlice(  0,  50, 1, 2, 3);
        DistributedSlice slice1b = new DistributedSlice( 50, 100, 2, 3, 4);
        DistributedSlice slice1c = new DistributedSlice(100, 150, 5, 6, 7);
        DistributedSlice slice1d = new DistributedSlice(150, 200, 0, 1, 2, 3, 4);

        child1.sendNext("0", functionWindowIdToString(windowId11), "1", new HolisticWindowAggregate(Arrays.asList(slice1a, slice1b)).asString());
        child1.sendNext("0", functionWindowIdToString(windowId21), "1", new HolisticWindowAggregate(Arrays.asList(slice1c)).asString());
        child1.sendNext("0", functionWindowIdToString(windowId31), "1", new HolisticWindowAggregate(Arrays.asList(slice1d)).asString());
        child1.sendNext(STREAM_END, "0");

        DistributedSlice slice2a = new DistributedSlice(  0,  50, 4, 5, 2, 3);
        DistributedSlice slice2b = new DistributedSlice( 50, 100, 10, 20, 30);
        DistributedSlice slice2c = new DistributedSlice(100, 150, 15, 25, 35);
        DistributedSlice slice2d = new DistributedSlice(150, 200, Collections.emptyList());

        child2.sendNext("1", functionWindowIdToString(windowId12), "1", new HolisticWindowAggregate(Arrays.asList(slice2a, slice2b)).asString());
        child2.sendNext("1", functionWindowIdToString(windowId22), "1", new HolisticWindowAggregate(Arrays.asList(slice2c)).asString());
        child2.sendNext("1", functionWindowIdToString(windowId32), "1", new HolisticWindowAggregate(Arrays.asList(slice2d)).asString());
        child2.sendNext(STREAM_END, "1");

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), window1);
        assertThat(result1.get(1), equalTo("3"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), window2);
        assertThat(result2.get(1), equalTo("10"));

        List<String> result3 = resultListener.receiveNext(2);
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

        String child1Id = "0";
        String child2Id = "1";
        String child3Id = "2";
        String child4Id = "3";
        String child5Id = "4";

        FunctionWindowAggregateId windowId1 = getDefaultFunctionWindowId(new WindowAggregateId(0, 0, 100));
        FunctionWindowAggregateId windowId2 = getDefaultFunctionWindowId(new WindowAggregateId(0, 100, 200));

        child1.sendNext(child1Id, functionWindowIdToString(windowId1), "1", new DistributiveWindowAggregate(5).asString());
        child1.sendNext(child1Id, functionWindowIdToString(windowId2), "1", new DistributiveWindowAggregate(7).asString());
        child1.sendNext(STREAM_END, child1Id);

        child2.sendNext(child2Id, functionWindowIdToString(windowId1), "1", new DistributiveWindowAggregate(3).asString());
        child2.sendNext(child2Id, functionWindowIdToString(windowId2), "1", new DistributiveWindowAggregate(4).asString());
        child2.sendNext(STREAM_END, child2Id);

        child3.sendNext(child3Id, functionWindowIdToString(windowId1), "1", new DistributiveWindowAggregate(1).asString());
        child3.sendNext(child3Id, functionWindowIdToString(windowId2), "1", new DistributiveWindowAggregate(0).asString());
        child3.sendNext(STREAM_END, child3Id);

        child4.sendNext(child4Id, functionWindowIdToString(windowId1), "1", new DistributiveWindowAggregate(10).asString());
        child4.sendNext(child4Id, functionWindowIdToString(windowId2), "1", new DistributiveWindowAggregate(100).asString());
        child4.sendNext(STREAM_END, child4Id);

        child5.sendNext(child5Id, functionWindowIdToString(windowId1), "1", new DistributiveWindowAggregate(0).asString());
        child5.sendNext(child5Id, functionWindowIdToString(windowId2), "1", new DistributiveWindowAggregate(3).asString());
        child5.sendNext(STREAM_END, child5Id);

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

        child1.sendNext("0", functionWindowIdToString(windowId1), "1", new AlgebraicWindowAggregate(new PartialAverage(6, 2)).asString());
        child1.sendNext("0", functionWindowIdToString(windowId2), "1", new AlgebraicWindowAggregate(new PartialAverage(14, 4)).asString());
        child1.sendNext(STREAM_END, "0");

        child2.sendNext("1", functionWindowIdToString(windowId1), "1", new AlgebraicWindowAggregate(new PartialAverage(9, 3)).asString());
        child2.sendNext("1", functionWindowIdToString(windowId2), "1", new AlgebraicWindowAggregate(new PartialAverage(10, 2)).asString());
        child2.sendNext(STREAM_END, "1");

        child3.sendNext("2", functionWindowIdToString(windowId1), "1", new AlgebraicWindowAggregate(new PartialAverage(1, 2)).asString());
        child3.sendNext("2", functionWindowIdToString(windowId2), "1", new AlgebraicWindowAggregate(new PartialAverage(0, 0)).asString());
        child3.sendNext(STREAM_END, "2");

        child4.sendNext("3", functionWindowIdToString(windowId1), "1", new AlgebraicWindowAggregate(new PartialAverage(10, 3)).asString());
        child4.sendNext("3", functionWindowIdToString(windowId2), "1", new AlgebraicWindowAggregate(new PartialAverage(10, 2)).asString());
        child4.sendNext(STREAM_END, "3");

        child5.sendNext("4", functionWindowIdToString(windowId1), "1", new AlgebraicWindowAggregate(new PartialAverage(150, 2)).asString());
        child5.sendNext("4", functionWindowIdToString(windowId2), "1", new AlgebraicWindowAggregate(new PartialAverage(25, 20)).asString());
        child5.sendNext(STREAM_END, "4");

        List<String> result1 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("14"));

        List<String> result2 = resultListener.receiveNext(2);
        assertFunctionWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("2"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenSessionMedian() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("SESSION,100,0"), Collections.singletonList("MEDIAN"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        int childId1 = 0;
        int childId2 = 1;

        FunctionWindowAggregateId windowId11 = new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 110), 0, childId1);
        FunctionWindowAggregateId windowId12 = new FunctionWindowAggregateId(new WindowAggregateId(0, 120, 270), 0, childId1);
        FunctionWindowAggregateId windowId13 = new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 510), 0, childId1);
        FunctionWindowAggregateId windowId14 = new FunctionWindowAggregateId(new WindowAggregateId(0, 550, 660), 0, childId1);
        FunctionWindowAggregateId windowId15 = new FunctionWindowAggregateId(new WindowAggregateId(0, 730, 830), 0, childId1);

        FunctionWindowAggregateId windowId21 = new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 130), 0, childId2);
        FunctionWindowAggregateId windowId22 = new FunctionWindowAggregateId(new WindowAggregateId(0, 460, 690), 0, childId2);
        FunctionWindowAggregateId windowId23 = new FunctionWindowAggregateId(new WindowAggregateId(0, 700, 850), 0, childId2);

        DistributedSlice slice11a = new DistributedSlice(  0,   4, 1, 2,  3);
        DistributedSlice slice11b = new DistributedSlice(  5,  10, 4, 5,  6);
        DistributedSlice slice12 =  new DistributedSlice(120, 170, 0, 5, 10);
        DistributedSlice slice13 =  new DistributedSlice(400, 410, 0, 5, 15);
        DistributedSlice slice14 =  new DistributedSlice(550, 560, 100, 0);
        DistributedSlice slice15 =  new DistributedSlice(730, 730, 0);

        DistributedSlice slice21 =  new DistributedSlice(  0,  30,  1, 2,  3);
        DistributedSlice slice22 =  new DistributedSlice(460, 590, 15, 5, 20);
        DistributedSlice slice23 =  new DistributedSlice(700, 750, 100, 0);

        child1.sendNext("0", functionWindowIdToString(windowId11), "1", new HolisticWindowAggregate(Arrays.asList(slice11a, slice11b)).asString());
        Thread.sleep(100);
        child2.sendNext("1", functionWindowIdToString(windowId21), "1", new HolisticWindowAggregate(Arrays.asList(slice21)).asString());
        Thread.sleep(100);
        child1.sendNext("0", functionWindowIdToString(windowId12), "1", new HolisticWindowAggregate(Arrays.asList(slice12)).asString());
        Thread.sleep(100);
        child1.sendNext("0", functionWindowIdToString(windowId13), "1", new HolisticWindowAggregate(Arrays.asList(slice13)).asString());
        Thread.sleep(100);
        child2.sendNext("1", functionWindowIdToString(windowId22), "1", new HolisticWindowAggregate(Arrays.asList(slice22)).asString());
        Thread.sleep(100);
        child1.sendNext("0", functionWindowIdToString(windowId14), "1", new HolisticWindowAggregate(Arrays.asList(slice14)).asString());
        Thread.sleep(100);
        child1.sendNext("0", functionWindowIdToString(windowId15), "1", new HolisticWindowAggregate(Arrays.asList(slice15)).asString());
        Thread.sleep(100);
        child1.sendNext(STREAM_END, "0");
        Thread.sleep(100);
        child2.sendNext("1", functionWindowIdToString(windowId23), "1", new HolisticWindowAggregate(Arrays.asList(slice23)).asString());
        Thread.sleep(100);
        child2.sendNext(STREAM_END, "1");

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 270), 0),  3L),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 690), 0), 15L)
        );

        List<List<String>> windowStrings = receiveResultWindows(windowMatchers.size(), resultListener);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));
        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenCountAndTimeWindow() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Arrays.asList("TUMBLING,3,0,COUNT", "TUMBLING,30,1"), Collections.singletonList("SUM"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        int childId1 = 0;
        int childId2 = 1;
        FunctionWindowAggregateId windowId11 = new FunctionWindowAggregateId(new WindowAggregateId(0,  0, 30), 0, childId1);
        FunctionWindowAggregateId windowId12 = new FunctionWindowAggregateId(new WindowAggregateId(0,  0, 30), 0, childId2);
        FunctionWindowAggregateId windowId21 = new FunctionWindowAggregateId(new WindowAggregateId(0, 30, 60), 0, childId1);
        FunctionWindowAggregateId windowId22 = new FunctionWindowAggregateId(new WindowAggregateId(0, 30, 60), 0, childId2);
        FunctionWindowAggregateId windowId31 = new FunctionWindowAggregateId(new WindowAggregateId(0, 60, 90), 0, childId1);
        FunctionWindowAggregateId windowId32 = new FunctionWindowAggregateId(new WindowAggregateId(0, 60, 90), 0, childId2);

        child1.sendNext(EVENT_STRING, "0,10,1");
        Thread.sleep(50);
        child2.sendNext(EVENT_STRING, "1,20,2");
        Thread.sleep(50);
        child1.sendNext(EVENT_STRING, "0,30,3");
        Thread.sleep(50);
        child2.sendNext(EVENT_STRING, "1,40,4");
        Thread.sleep(50);
        child1.sendNext(EVENT_STRING, "0,50,5");
        Thread.sleep(50);
        child2.sendNext(EVENT_STRING, "1,60,6");
        Thread.sleep(50);
        child1.sendNext("0", functionWindowIdToString(windowId11), "1", new DistributiveWindowAggregate(1).asString());
        Thread.sleep(50);
        child1.sendNext(EVENT_STRING, "0,70,7");
        Thread.sleep(50);
        child2.sendNext("1", functionWindowIdToString(windowId12), "1", new DistributiveWindowAggregate(2).asString());
        Thread.sleep(50);
        child2.sendNext(EVENT_STRING, "1,80,8");
        Thread.sleep(50);
        child1.sendNext(EVENT_STRING, "0,90,9");
        Thread.sleep(50);
        child1.sendNext("0", functionWindowIdToString(windowId21), "1", new DistributiveWindowAggregate(8).asString());
        Thread.sleep(50);
        child1.sendNext("0", functionWindowIdToString(windowId31), "1", new DistributiveWindowAggregate(7).asString());
        Thread.sleep(50);
        child1.sendNext(STREAM_END, "0");
        Thread.sleep(50);
        child2.sendNext("1", functionWindowIdToString(windowId22), "1", new DistributiveWindowAggregate(4).asString());
        Thread.sleep(50);
        child2.sendNext("1", functionWindowIdToString(windowId32), "1", new DistributiveWindowAggregate(14).asString());
        Thread.sleep(50);
        child2.sendNext(EVENT_STRING, "1,200,0");
        child2.sendNext(EVENT_STRING, "1,201,0");
        Thread.sleep(50);
        child2.sendNext(STREAM_END, "1");

        List<Matcher<? super List<String>>> windowResultMatchers = Arrays.asList(
                // Count 0-3
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,  0,  3), 0),  6L),
                // Time 0-30
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,  0, 30), 0),  3L),
                // Count 3-6
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,  3,  6), 0), 15L),
                // Time 30-60
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 30, 60), 0), 12L),
                // Time 60-90
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 60, 90), 0), 21L)
        );

        List<List<String>> windowStrings = new ArrayList<>(windowResultMatchers.size());
        for (int i = 0; i < windowResultMatchers.size(); i++) {
            windowStrings.add(resultListener.receiveNext(2));
        }

        System.out.println(windowStrings);
        assertThat(windowStrings, containsInAnyOrder(windowResultMatchers));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }

    @Test
    void testTwoChildrenSumAvgMedianAggregatesMultiKey() throws Exception {
        int numChildren = 2;
        DistributedRoot root = runRoot(numChildren,
                Collections.singletonList("TUMBLING,100,1"), Arrays.asList("SUM", "AVG", "MEDIAN"));

        ZMQPushMock child1 = children.get(0);
        ZMQPushMock child2 = children.get(1);

        String child1Id = "0";
        String child2Id = "1";

        WindowAggregateId window1 = new WindowAggregateId(0,   0, 100);
        WindowAggregateId window2 = new WindowAggregateId(0, 100, 200);

        // Window 1
        FunctionWindowAggregateId sumChild1Window1Id = new FunctionWindowAggregateId(window1, 0, 1);
        FunctionWindowAggregateId sumChild2Window1Id = new FunctionWindowAggregateId(window1, 0, 2);

        FunctionWindowAggregateId avgChild1Window1Id = new FunctionWindowAggregateId(window1, 1, 1);
        FunctionWindowAggregateId avgChild2Window1Id = new FunctionWindowAggregateId(window1, 1, 2);

        FunctionWindowAggregateId medChild1Window1Id = new FunctionWindowAggregateId(window1, 2, 1);
        FunctionWindowAggregateId medChild2Window1Id = new FunctionWindowAggregateId(window1, 2, 2);

        // Window 2
        FunctionWindowAggregateId sumChild1Window2Id = new FunctionWindowAggregateId(window2, 0, 1);
        FunctionWindowAggregateId sumChild2Window2Id = new FunctionWindowAggregateId(window2, 0, 2);

        FunctionWindowAggregateId avgChild1Window2Id = new FunctionWindowAggregateId(window2, 1, 1);
        FunctionWindowAggregateId avgChild2Window2Id = new FunctionWindowAggregateId(window2, 1, 2);

        FunctionWindowAggregateId medChild1Window2Id = new FunctionWindowAggregateId(window2, 2, 1);
        FunctionWindowAggregateId medChild2Window2Id = new FunctionWindowAggregateId(window2, 2, 2);

        // Window 1
        DistributedSlice sliceW1C1K1a = new DistributedSlice(  0,  50, Arrays.asList(1L, 2L, 3L), 1);
        DistributedSlice sliceW1C1K1b = new DistributedSlice( 50, 100, Arrays.asList(4L, 5L, 6L), 1);
        DistributedSlice sliceW1C1K2 =  new DistributedSlice(  0, 100, Arrays.asList(1L, 2L, 3L), 2);

        DistributedSlice sliceW1C2K1a = new DistributedSlice(  0,  50, Arrays.asList(7L, 8L, 9L), 1);
        DistributedSlice sliceW1C2K1b = new DistributedSlice( 50, 100, Arrays.asList(0L, 9L, 7L), 1);
        DistributedSlice sliceW1C2K2 =  new DistributedSlice(  0, 100, Arrays.asList(0L, 9L, 7L), 2);

        // Window 2
        DistributedSlice sliceW2C1K1 =  new DistributedSlice(100, 200, Arrays.asList(7L, 9L, 0L), 1);
        DistributedSlice sliceW2C1K2 =  new DistributedSlice(100, 200, Arrays.asList(7L, 8L, 9L), 2);

        DistributedSlice sliceW2C2K1 =  new DistributedSlice(100, 200, Arrays.asList(1L, 2L, 3L), 1);
        DistributedSlice sliceW2C2K2 =  new DistributedSlice(100, 200, Arrays.asList(5L, 6L, 7L), 2);

        List<List<String>> child1Messages = Arrays.asList(
                // Window 1
                Arrays.asList(child1Id, functionWindowIdToString(sumChild1Window1Id), "2",
                        new DistributiveWindowAggregate(21, 1).asString(),
                        new DistributiveWindowAggregate( 6, 2).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(avgChild1Window1Id), "2",
                        new AlgebraicWindowAggregate(new PartialAverage(21, 6), 1).asString(),
                        new AlgebraicWindowAggregate(new PartialAverage( 6, 3), 2).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(medChild1Window1Id), "2",
                        new HolisticWindowAggregate(Arrays.asList(sliceW1C1K1a, sliceW1C1K1b), 1).asString(),
                        new HolisticWindowAggregate(Arrays.asList(sliceW1C1K2),                2).asString()),

                // Window 2
                Arrays.asList(child1Id, functionWindowIdToString(sumChild1Window2Id), "2",
                        new DistributiveWindowAggregate(16, 1).asString(),
                        new DistributiveWindowAggregate(24, 2).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(avgChild1Window2Id), "2",
                        new AlgebraicWindowAggregate(new PartialAverage(16, 3), 1).asString(),
                        new AlgebraicWindowAggregate(new PartialAverage(24, 3), 2).asString()),

                Arrays.asList(child1Id, functionWindowIdToString(medChild1Window2Id), "2",
                        new HolisticWindowAggregate(Arrays.asList(sliceW2C1K1), 1).asString(),
                        new HolisticWindowAggregate(Arrays.asList(sliceW2C1K2), 2).asString()),

                Arrays.asList(STREAM_END, child1Id)
        );

        for (List<String> msg : child1Messages) {
            child1.sendNext(msg);
        }

        List<List<String>> child2Messages = Arrays.asList(
                // Window 1
                Arrays.asList(child2Id, functionWindowIdToString(sumChild2Window1Id), "2",
                        new DistributiveWindowAggregate(40, 1).asString(),
                        new DistributiveWindowAggregate(16, 2).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(avgChild2Window1Id), "2",
                        new AlgebraicWindowAggregate(new PartialAverage(40, 6), 1).asString(),
                        new AlgebraicWindowAggregate(new PartialAverage(16, 3), 2).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(medChild2Window1Id), "2",
                        new HolisticWindowAggregate(Arrays.asList(sliceW1C2K1a, sliceW1C2K1b), 1).asString(),
                        new HolisticWindowAggregate(Arrays.asList(sliceW1C2K2),                2).asString()),

                // Window 2
                Arrays.asList(child2Id, functionWindowIdToString(sumChild2Window2Id), "2",
                        new DistributiveWindowAggregate( 6, 1).asString(),
                        new DistributiveWindowAggregate(18, 2).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(avgChild2Window2Id), "2",
                        new AlgebraicWindowAggregate(new PartialAverage( 6, 3), 1).asString(),
                        new AlgebraicWindowAggregate(new PartialAverage(18, 3), 2).asString()),

                Arrays.asList(child2Id, functionWindowIdToString(medChild2Window2Id), "2",
                        new HolisticWindowAggregate(Arrays.asList(sliceW2C2K1), 1).asString(),
                        new HolisticWindowAggregate(Arrays.asList(sliceW2C2K2), 2).asString()),

                Arrays.asList(STREAM_END, child2Id)
        );

        for (List<String> msg : child2Messages) {
            child2.sendNext(msg);
        }

        List<Matcher<? super List<String>>> windowResultMatchers = Arrays.asList(
                // Window 1, key 1
                equalsWindowResult(new FunctionWindowAggregateId(window1, 0, NO_CHILD_ID, 1), 61L), // sum, window 1, key 1
                equalsWindowResult(new FunctionWindowAggregateId(window1, 1, NO_CHILD_ID, 1),  5L), // avg, window 1, key 1
                equalsWindowResult(new FunctionWindowAggregateId(window1, 2, NO_CHILD_ID, 1),  6L), // med, window 1, key 1

                // Window 1, key 2
                equalsWindowResult(new FunctionWindowAggregateId(window1, 0, NO_CHILD_ID, 2), 22L), // sum, window 1, key 2
                equalsWindowResult(new FunctionWindowAggregateId(window1, 1, NO_CHILD_ID, 2),  3L), // avg, window 1, key 2
                equalsWindowResult(new FunctionWindowAggregateId(window1, 2, NO_CHILD_ID, 2),  3L), // med, window 1, key 2

                // Window 2, key 1
                equalsWindowResult(new FunctionWindowAggregateId(window2, 0, NO_CHILD_ID, 1), 22L), // sum, window 2, key 1
                equalsWindowResult(new FunctionWindowAggregateId(window2, 1, NO_CHILD_ID, 1),  3L), // avg, window 2, key 1
                equalsWindowResult(new FunctionWindowAggregateId(window2, 2, NO_CHILD_ID, 1),  3L), // med, window 2, key 1

                // Window 2, key 2
                equalsWindowResult(new FunctionWindowAggregateId(window2, 0, NO_CHILD_ID, 2), 42L), // sum, window 2, key 2
                equalsWindowResult(new FunctionWindowAggregateId(window2, 1, NO_CHILD_ID, 2),  7L), // avg, window 2, key 2
                equalsWindowResult(new FunctionWindowAggregateId(window2, 2, NO_CHILD_ID, 2),  7L)  // med, window 2, key 2

        );

        List<List<String>> windowStrings = new ArrayList<>(windowResultMatchers.size());
        for (int i = 0; i < windowResultMatchers.size(); i++) {
            windowStrings.add(resultListener.receiveNext(2));
        }

        assertThat(windowStrings, containsInAnyOrder(windowResultMatchers));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }
}
