package com.github.lawben.disco.integration;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.DistributedUtils.windowIdToString;

import com.github.lawben.disco.DistributedUtils;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.lawben.disco.DistributedRoot;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import com.github.lawben.disco.utils.ZMQRequestMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
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

        File tempFile = File.createTempFile("disco-", "-test");
        tempFile.deleteOnExit();
        this.resultPath = tempFile.getAbsolutePath();

        this.children = new ArrayList<>();
        this.resultListener = new ZMQPullMock(this.resultPath);
    }

    @AfterEach
    public void tearDown() {

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

    void assertWindowIdStringEquals(String windowString, WindowAggregateId windowId) {
        String expectedWindowString = windowId.getWindowId() + "," + windowId.getWindowStartTimestamp() +
                "," + windowId.getWindowEndTimestamp();
        assertEquals(expectedWindowString, windowString);
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

        WindowAggregateId windowId1 = new WindowAggregateId(0, 0, 100);
        WindowAggregateId windowId2 = new WindowAggregateId(0, 100, 200);
        child.addMessage("0", windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING,"5");
        child.addMessage("0", windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "7");
        child.addMessage(STREAM_END, "0");
        child.sendNext();
        child.sendNext();
        child.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("5"));

        List<String> result2 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result2.get(0), windowId2);
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

        WindowAggregateId windowId1 = new WindowAggregateId(0, 0, 100);
        WindowAggregateId windowId2 = new WindowAggregateId(0, 100, 200);
        child.addMessage("0", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "6,2");
        child.addMessage("0", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "8,4");
        child.addMessage(STREAM_END, "0");
        child.sendNext();
        child.sendNext();
        child.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("3"));

        List<String> result2 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("2"));

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

        WindowAggregateId windowId1 = new WindowAggregateId(0, 0, 100);
        WindowAggregateId windowId2 = new WindowAggregateId(0, 100, 200);

        child1.addMessage(child1Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "5");
        child1.addMessage(child1Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "7");
        child1.addMessage(STREAM_END, child1Id);
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage(child2Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "3");
        child2.addMessage(child2Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "4");
        child2.addMessage(STREAM_END, child2Id);
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("8"));

        List<String> result2 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("11"));

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

        WindowAggregateId windowId1 = new WindowAggregateId(0, 0, 100);
        WindowAggregateId windowId2 = new WindowAggregateId(0, 100, 200);
        child1.addMessage("0", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "6,2");
        child1.addMessage("0", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "14,4");
        child1.addMessage(STREAM_END, "0");
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage("1", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "9,3");
        child2.addMessage("1", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "10,2");
        child2.addMessage(STREAM_END, "1");
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("3"));

        List<String> result2 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("4"));

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

        WindowAggregateId windowId1 = new WindowAggregateId(0, 0, 100);
        WindowAggregateId windowId2 = new WindowAggregateId(0, 100, 200);

        child1.addMessage(child1Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "5");
        child1.addMessage(child1Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "7");
        child1.addMessage(STREAM_END, child1Id);
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage(child2Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "3");
        child2.addMessage(child2Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "4");
        child2.addMessage(STREAM_END, child2Id);
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        child3.addMessage(child3Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "1");
        child3.addMessage(child3Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "0");
        child3.addMessage(STREAM_END, child3Id);
        child3.sendNext();
        child3.sendNext();
        child3.sendNext();

        child4.addMessage(child4Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "10");
        child4.addMessage(child4Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "100");
        child4.addMessage(STREAM_END, child4Id);
        child4.sendNext();
        child4.sendNext();
        child4.sendNext();

        child5.addMessage(child5Id, windowIdToString(windowId1), DistributedUtils.DISTRIBUTIVE_STRING, "0");
        child5.addMessage(child5Id, windowIdToString(windowId2), DistributedUtils.DISTRIBUTIVE_STRING, "3");
        child5.addMessage(STREAM_END, child5Id);
        child5.sendNext();
        child5.sendNext();
        child5.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("19"));

        List<String> result2 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result2.get(0), windowId2);
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

        WindowAggregateId windowId1 = new WindowAggregateId(0, 0, 100);
        WindowAggregateId windowId2 = new WindowAggregateId(0, 100, 200);

        child1.addMessage("1", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "6,2");
        child1.addMessage("1", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "14,4");
        child1.addMessage(STREAM_END, "1");
        child1.sendNext();
        child1.sendNext();
        child1.sendNext();

        child2.addMessage("2", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "9,3");
        child2.addMessage("2", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "10,2");
        child2.addMessage(STREAM_END, "2");
        child2.sendNext();
        child2.sendNext();
        child2.sendNext();

        child3.addMessage("3", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "1,2");
        child3.addMessage("3", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "0,0");
        child3.addMessage(STREAM_END, "3");
        child3.sendNext();
        child3.sendNext();
        child3.sendNext();

        child4.addMessage("4", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "10,3");
        child4.addMessage("4", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "10,2");
        child4.addMessage(STREAM_END, "4");
        child4.sendNext();
        child4.sendNext();
        child4.sendNext();

        child5.addMessage("5", windowIdToString(windowId1), DistributedUtils.ALGEBRAIC_STRING, "150,2");
        child5.addMessage("5", windowIdToString(windowId2), DistributedUtils.ALGEBRAIC_STRING, "25,20");
        child5.addMessage(STREAM_END, "5");
        child5.sendNext();
        child5.sendNext();
        child5.sendNext();

        List<String> result1 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result1.get(0), windowId1);
        assertThat(result1.get(1), equalTo("14"));

        List<String> result2 = resultListener.receiveNext(2);
        assertWindowIdStringEquals(result2.get(0), windowId2);
        assertThat(result2.get(1), equalTo("2"));

        assertRootEnd();
        assertNoFinalThreadException(root);
    }
}
