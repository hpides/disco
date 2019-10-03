package com.github.lawben.disco.integration;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.ARG_DELIMITER;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.utils.SessionControlMatcher.equalsSessionStart;
import static com.github.lawben.disco.utils.TestUtils.receiveWindow;
import static com.github.lawben.disco.utils.TestUtils.receiveWindows;
import static com.github.lawben.disco.utils.TestUtils.runThread;
import static com.github.lawben.disco.utils.TestUtils.sendSleepSortedEvents;
import static com.github.lawben.disco.utils.TestUtils.sendSortedEvents;
import static com.github.lawben.disco.utils.WindowMatcher.equalsWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.AlgebraicWindowAggregate;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.DistributiveWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.HolisticWindowAggregate;
import com.github.lawben.disco.aggregation.functions.PartialAverage;
import com.github.lawben.disco.utils.EventMatcher;
import com.github.lawben.disco.utils.ExpectedWindow;
import com.github.lawben.disco.utils.WindowMatcher;
import com.github.lawben.disco.utils.ZMQMock;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import com.github.lawben.disco.utils.ZMQRequestMock;
import com.github.lawben.disco.utils.ZMQRespondMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.Utils;

public class DistributedChildTest {
    private final static String START_TIME = "1570061857";
    private int childPort;
    private int childId;
    private String rootIp;

    private ZMQRespondMock rootRegisterResponder;
    private ZMQPullMock rootWindowReceiver;
    private List<ZMQPushMock> streamSenders;

    @BeforeEach
    public void setup() throws IOException {
        while ((this.childPort = Utils.findOpenPort()) > 65536 - 100) {
            this.childPort = Utils.findOpenPort();
        }
        this.childId = 0;
        this.rootIp = "localhost";
        this.rootRegisterResponder = null;
        this.rootWindowReceiver = null;
        this.streamSenders = new ArrayList<>();
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeIfNotNull(this.rootRegisterResponder);
        closeIfNotNull(this.rootWindowReceiver);

        for (ZMQMock mock : this.streamSenders) {
            closeIfNotNull(mock);
        }
    }

    void closeIfNotNull(AutoCloseable x) throws Exception {
        if (x != null) {
            x.close();
        }
    }

    DistributedChild defaultChild(int rootInitPort, int rootWindowPort) throws IOException {
        return defaultChild(rootInitPort, rootWindowPort, 1);
    }

    DistributedChild defaultChild(int rootInitPort, int rootWindowPort, int numStreams) throws IOException {
        if (rootWindowPort == 0) {
            rootWindowPort = Utils.findOpenPort();
        }
        return new DistributedChild(rootIp, rootInitPort, rootWindowPort, childPort, childId, numStreams);
    }

    DistributedChild runDefaultChild(int rootInitPort, int rootWindowPort) throws IOException {
        return runDefaultChild(rootInitPort, rootWindowPort, 1);
    }

    DistributedChild runDefaultChild(int rootInitPort, int rootWindowPort, int numStreams) throws IOException {
        DistributedChild child = defaultChild(rootInitPort, rootWindowPort, numStreams);
        runThread(child);
        return child;
    }

    FunctionWindowAggregateId defaultFunctionWindowId(WindowAggregateId windowAggregateId) {
        return new FunctionWindowAggregateId(windowAggregateId, 0);
    }

    void assertChildEnd() {
        List<String> childEnd = rootWindowReceiver.receiveNext(2);
        assertThat(childEnd, hasSize(2));
        Assertions.assertEquals(DistributedUtils.STREAM_END, childEnd.get(0));
        assertEquals(String.valueOf(childId), childEnd.get(1));
    }

    DistributedChild defaultInit() throws Exception {
        return defaultInit(1);
    }

    DistributedChild defaultInit(int numStreams) throws Exception {
        int rootInitPort = Utils.findOpenPort();
        try (ZMQRespondMock rootRegisterResponder = new ZMQRespondMock(rootInitPort)) {
            rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0", "SUM");

            int rootWindowPort = Utils.findOpenPort();
            rootWindowReceiver = new ZMQPullMock(rootWindowPort);

            DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, numStreams);
            rootRegisterResponder.respondToNext();
            Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

            for (int i = 0; i < numStreams; i++) {
                registerStream(i);
            }

            Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
            return child;
        }
    }

    ZMQPushMock registerStream(int streamId) {
        String streamIdString = String.valueOf(streamId);
        try (ZMQRequestMock streamRegister = new ZMQRequestMock(childPort + STREAM_REGISTER_PORT_OFFSET)) {
            streamRegister.addMessage(streamIdString);

            List<String> registerResult = streamRegister.requestNext();
            assertThat(registerResult, not(empty()));
            assertThat(registerResult.get(0), equalTo(START_TIME));
        }

        ZMQPushMock stream = new ZMQPushMock(childPort);
        streamSenders.add(stream);
        return stream;
    }

    @Test
    void testRegisterChild() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100", "SUM");

        runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new node"));

    }

    @Test
    void testRegisterChildMultiWindowSum() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0" + ARG_DELIMITER + "SLIDING,100,50,1" + ARG_DELIMITER + "SESSION,100,2", "SUM");

        runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new node"));

    }

    @Test
    void testRegisterChildMultiWindowAvg() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0" + ARG_DELIMITER + "SLIDING,100,50,1" + ARG_DELIMITER + "SESSION,100,2", "AVG");

        runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new node"));

    }

    @Test
    void testRegisterChildMultiWindowMedian() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0" + ARG_DELIMITER + "SLIDING,100,50,1" + ARG_DELIMITER + "SESSION,100,2", "MEDIAN");

        runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new node"));

    }

    @Test
    void testOneStreamRegister() throws Exception {
        defaultInit();
    }

    @Test
    void testTwoStreamsRegister() throws Exception {
        defaultInit(2);
    }

    @Test
    void testSingleEventSingleStream() throws Exception {
        defaultInit();

        sendSortedEvents(streamSenders, new String[]{"0,1,1"});
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        ExpectedWindow expectedWindow = new ExpectedWindow(childId,
                defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), new DistributiveWindowAggregate(1));

        List<String> windowString = receiveWindow(rootWindowReceiver);
        assertThat(windowString, equalsWindow(expectedWindow));

        assertChildEnd();
    }

    @Test
    void testFiveEventsSingleStream() throws Exception {
        defaultInit();

        String[] events = { "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1" };
        sendSortedEvents(streamSenders, events);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> windowString = receiveWindow(rootWindowReceiver);
        ExpectedWindow expectedWindow1 = new ExpectedWindow(0,
                defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), new DistributiveWindowAggregate(5));
        assertThat(windowString, equalsWindow(expectedWindow1));

        assertChildEnd();
    }

    @Test
    void testTwoWindowsSingleStream() throws Exception {
        defaultInit();
        String[] events = {
                "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1",  // window 1
                "0,110,10", "0,120,20", "0,130,30", "0,140,40", "0,150,50",  // window 2
        };
        sendSortedEvents(streamSenders, events);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> window1String = receiveWindow(rootWindowReceiver);
        ExpectedWindow expectedWindow1 = new ExpectedWindow(0,
                defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), new DistributiveWindowAggregate(5));
        assertThat(window1String, equalsWindow(expectedWindow1));

        List<String> window2String = receiveWindow(rootWindowReceiver);
        ExpectedWindow expectedWindow2 = new ExpectedWindow(0,
                defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)), new DistributiveWindowAggregate(150));
        assertThat(window2String, equalsWindow(expectedWindow2));

        assertChildEnd();
    }

    @Test
    void testTwoWindowsTwoStreams() throws Exception {
        defaultInit(2);

        String[] events0 = {
                "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1",  // window 1
                "0,110,10", "0,120,20", "0,130,30", "0,140,40", "0,150,50",  // window 2
        };

        String[] events1 = {
                "1,10,1", "1,20,1", "1,30,1", "1,40,1", "1,50,1",  // window 1
                "1,110,10", "1,120,20", "1,130,30", "1,140,40", "1,150,50",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> window1String = receiveWindow(rootWindowReceiver);
        ExpectedWindow expectedWindow1 = new ExpectedWindow(0,
                defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), new DistributiveWindowAggregate(10));
        assertThat(window1String, equalsWindow(expectedWindow1));

        List<String> window2String = receiveWindow(rootWindowReceiver);
        ExpectedWindow expectedWindow2 = new ExpectedWindow(0,
                defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)), new DistributiveWindowAggregate(300));
        assertThat(window2String, equalsWindow(expectedWindow2));

        assertChildEnd();
    }

    @Test
    void testMultiWindowSumTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0" + ARG_DELIMITER + "SLIDING,100,50,1" + ARG_DELIMITER + "SESSION,60,2", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,0", "0,90,1,0",  // window 1
                "0,110,10,0", "0,115,20,0", "0,120,30,0", "0,190,40,0", "0,195,50,0",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,1", "1,60,1,1", "1,80,1,1", "1,85,1,1",  // window 1
                "1,185,10,1", "1,186,20,1", "1,187,30,1", "1,190,40,1", "1,195,50,1",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        final Long dummyNullValue = null;

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(0,   0, 100)),
                        new DistributiveWindowAggregate(5, 0), new DistributiveWindowAggregate(5, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(1,   0, 100)),
                        new DistributiveWindowAggregate(5, 0), new DistributiveWindowAggregate(5, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(2,  10, 180)),
                        new DistributiveWindowAggregate(65, 0)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(2,  20, 145)),
                        new DistributiveWindowAggregate(5, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(1,  50, 150)),
                        new DistributiveWindowAggregate(63, 0), new DistributiveWindowAggregate(3, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)),
                        new DistributiveWindowAggregate(150, 0), new DistributiveWindowAggregate(150, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(1, 100, 200)),
                        new DistributiveWindowAggregate(150, 0), new DistributiveWindowAggregate(150, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(1, 150, 250)),
                        new DistributiveWindowAggregate(90, 0), new DistributiveWindowAggregate(150, 1)),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(2, 185, 255)),
                        new DistributiveWindowAggregate(150, 1)),
                // This only exists because of the STREAM_END logic
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(2, 190, 255)),
                        new DistributiveWindowAggregate(dummyNullValue, 0))
        );

        List<Matcher<? super List<String>>> sessionStartMatchers = Arrays.asList(
                equalsSessionStart(FunctionWindowAggregateId.sessionStartId(2, 190, 0, 0)),
                equalsSessionStart(FunctionWindowAggregateId.sessionStartId(2, 185, 0, 1))

        );

        List<Matcher<? super List<String>>> allMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow).collect(Collectors.toList());
        allMatchers.addAll(sessionStartMatchers);

        List<List<String>> windowStrings = receiveWindows(allMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(allMatchers));

        assertChildEnd();
    }

    @Test
    void testAvgTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "SLIDING,100,50,0", "AVG");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events0 = {
                "0,10,1", "0,30,1", "0,50,1", "0,70,1", "0,90,1",  // window 1
                "0,110,10", "0,115,20", "0,120,30", "0,190,40", "0,195,50",  // window 2
        };

        String[] events1 = {
                "1,20,1", "1,40,1", "1,60,1", "1,80,1", "1,85,1",  // window 1
                "1,175,10", "1,180,20", "1,185,30", "1,190,40", "1,195,50",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(0,   0, 100)),
                        new AlgebraicWindowAggregate(new PartialAverage( 10, 10))),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(0,  50, 150)),
                        new AlgebraicWindowAggregate(new PartialAverage( 66,  9))),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)),
                        new AlgebraicWindowAggregate(new PartialAverage(300, 10))),
                new ExpectedWindow(childId, defaultFunctionWindowId(new WindowAggregateId(0, 150, 250)),
                        new AlgebraicWindowAggregate(new PartialAverage(240,  7)))
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
    }

    @Test
    void testMedianTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "SLIDING,100,50,0", "MEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,0", "0,90,1,0",  // window 1
                "0,110,10,0", "0,115,20,0", "0,120,30,0", "0,190,40,0", "0,195,50,0",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,1", "1,60,1,1", "1,80,1,1", "1,85,1,1",  // window 1
                "1,175,10,1", "1,180,20,1", "1,185,30,1", "1,190,40,1", "1,195,50,1",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  30, 1, 1),
                new DistributedSlice( 50,  90, 1, 1, 1),
                new DistributedSlice(100, 120, 10, 20, 30),
                new DistributedSlice(150, 195, 40, 50)
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  40, 1, 1),
                new DistributedSlice( 50,  85, 1, 1, 1),
                new DistributedSlice(150, 195, 10, 20, 30, 40, 50)
        );

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 100), 0, childId),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(0), stream0Slices.get(1)), 0),
                        new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(0), stream1Slices.get(1)), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,  50, 150), 0, childId),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(2)), 0),
                        new HolisticWindowAggregate(Collections.emptyList(), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 100, 200), 0, childId),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(3)), 0),
                        new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(2)), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 150, 250), 0, childId),
                        new HolisticWindowAggregate(Collections.emptyList(), 0),
                        new HolisticWindowAggregate(Collections.emptyList(), 1))
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
    }

    @Test
    void testMedianTumblingAndSessionTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "SLIDING,100,50,0" + ARG_DELIMITER + "SESSION,60,1", "MEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,0", "0,90,1,0",  // window 1
                "0,110,10,0", "0,115,20,0", "0,120,30,0", "0,190,40,0", "0,195,50,0",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,1", "1,60,1,1", "1,80,1,1", "1,85,1,1",  // window 1
                "1,185,10,1", "1,186,20,1", "1,187,30,1", "1,190,40,1", "1,195,50,1",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice( 10,  30, 1, 1),
                new DistributedSlice( 50,  90, 1, 1, 1),
                new DistributedSlice(100, 120, 10, 20, 30),
                new DistributedSlice(150, 195, 40, 50)
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice( 20,  40, 1, 1),
                new DistributedSlice( 50,  85, 1, 1, 1),
                new DistributedSlice(150, 195, 10, 20, 30, 40, 50)
        );

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 100), 0, childId),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(0), stream0Slices.get(1)), 0),
                        new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(0), stream1Slices.get(1)), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(1,  20, 145), 0, childId),
                        new HolisticWindowAggregate(Collections.emptyList(), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(1,  10, 180), 0, childId),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(2)), 0)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,  50, 150), 0, childId),
                        new HolisticWindowAggregate(Collections.emptyList(), 0),
                        new HolisticWindowAggregate(Collections.emptyList(), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 100, 200), 0, childId),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(3)), 0),
                        new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(2)), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 150, 250), 0, childId),
                        new HolisticWindowAggregate(Collections.emptyList(), 0),
                        new HolisticWindowAggregate(Collections.emptyList(), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(1, 185, 255), 0, childId),
                        new HolisticWindowAggregate(Collections.emptyList(), 1)),

                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(1, 190, 255), 0, childId),
                        new HolisticWindowAggregate(Collections.emptyList(), 0))
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
    }

    @Test
    void testTwoStreamsSumAvgMedianAggregatesTwice() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0", "SUM" + ARG_DELIMITER + "SUM" + ARG_DELIMITER + "AVG" + ARG_DELIMITER + "AVG" + ARG_DELIMITER + "MEDIAN" + ARG_DELIMITER + "MEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,0", "0,90,1,0",  // window 1
                "0,110,10,0", "0,115,20,0", "0,120,30,0", "0,190,40,0", "0,195,50,0",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,1", "1,60,1,1", "1,80,1,1", "1,85,1,1",  // window 1
                "1,175,10,1", "1,180,20,1", "1,185,30,1", "1,190,40,1", "1,195,50,1",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  90, 1, 1, 1, 1, 1),
                new DistributedSlice(100, 195, 10, 20, 30, 40, 50)
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  85, 1, 1, 1, 1, 1),
                new DistributedSlice(100, 195, 10, 20, 30, 40, 50)
        );

        WindowAggregateId window1 = new WindowAggregateId(0,   0, 100);
        WindowAggregateId window2 = new WindowAggregateId(0, 100, 200);

        FunctionWindowAggregateId sum1Window1Id = new FunctionWindowAggregateId(window1, 0);
        FunctionWindowAggregateId sum2Window1Id = new FunctionWindowAggregateId(window1, 1);
        FunctionWindowAggregateId avg1Window1Id = new FunctionWindowAggregateId(window1, 2);
        FunctionWindowAggregateId avg2Window1Id = new FunctionWindowAggregateId(window1, 3);
        FunctionWindowAggregateId med1Window1Id = new FunctionWindowAggregateId(window1, 4);
        FunctionWindowAggregateId med2Window1Id = new FunctionWindowAggregateId(window1, 5);

        FunctionWindowAggregateId sum1Window2Id = new FunctionWindowAggregateId(window2, 0);
        FunctionWindowAggregateId sum2Window2Id = new FunctionWindowAggregateId(window2, 1);
        FunctionWindowAggregateId avg1Window2Id = new FunctionWindowAggregateId(window2, 2);
        FunctionWindowAggregateId avg2Window2Id = new FunctionWindowAggregateId(window2, 3);
        FunctionWindowAggregateId med1Window2Id = new FunctionWindowAggregateId(window2, 4);
        FunctionWindowAggregateId med2Window2Id = new FunctionWindowAggregateId(window2, 5);

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, sum1Window1Id, new DistributiveWindowAggregate(5, 0), new DistributiveWindowAggregate(5, 1)),
                new ExpectedWindow(childId, sum2Window1Id, new DistributiveWindowAggregate(5, 0), new DistributiveWindowAggregate(5, 1)),
                new ExpectedWindow(childId, avg1Window1Id, new AlgebraicWindowAggregate(new PartialAverage(5, 5), 0), new AlgebraicWindowAggregate(new PartialAverage(5, 5), 1)),
                new ExpectedWindow(childId, avg2Window1Id, new AlgebraicWindowAggregate(new PartialAverage(5, 5), 0), new AlgebraicWindowAggregate(new PartialAverage(5, 5), 1)),
                new ExpectedWindow(childId, med1Window1Id, new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(0)), 0), new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(0)), 1)),
                new ExpectedWindow(childId, med2Window1Id, new HolisticWindowAggregate(Arrays.asList(), 0), new HolisticWindowAggregate(Arrays.asList(), 1)),
                new ExpectedWindow(childId, sum1Window2Id, new DistributiveWindowAggregate(150, 0), new DistributiveWindowAggregate(150, 1)),
                new ExpectedWindow(childId, sum2Window2Id, new DistributiveWindowAggregate(150, 0), new DistributiveWindowAggregate(150, 1)),
                new ExpectedWindow(childId, avg1Window2Id, new AlgebraicWindowAggregate(new PartialAverage(150, 5), 0), new AlgebraicWindowAggregate(new PartialAverage(150, 5), 1)),
                new ExpectedWindow(childId, avg2Window2Id, new AlgebraicWindowAggregate(new PartialAverage(150, 5), 0), new AlgebraicWindowAggregate(new PartialAverage(150, 5), 1)),
                new ExpectedWindow(childId, med1Window2Id, new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(1)), 0), new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(1)), 1)),
                new ExpectedWindow(childId, med2Window2Id, new HolisticWindowAggregate(Arrays.asList(), 0), new HolisticWindowAggregate(Arrays.asList(), 1))
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow)
                .collect(Collectors.toList());

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
    }

    @Test
    void testTwoStreamsSessionMedian() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "SESSION,100,0", "MEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        int streamId1 = 0;
        int streamId2 = 1;
        String[] events0 = {
                "0,0,1,0", "0,1,2,0", "0,4,3,0", "0,5,4,0", "0,6,5,0", "0,10,6,0",
                "0,120,0,0", "0,140,5,0", "0,170,10,0",
                "0,400,0,0", "0,405,5,0", "0,410,15,0",
                "0,550,100,0", "0,560,0,0"
        };

        String[] events1 = {
                "1,0,1,1", "1,15,2,1", "1,30,3,1",
                "1,460,15,1", "1,500,5,1", "1,590,20,1",
                "1,700,100,1", "1,750,0,1"
        };

        sendSleepSortedEvents(100, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  10, 1, 2, 3, 4, 5, 6),
                new DistributedSlice(120, 170, 0, 5, 10),
                new DistributedSlice(400, 410, 0, 5, 15),
                new DistributedSlice(550, 560, 100, 0)
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  30,  1, 2,  3),
                new DistributedSlice(460, 590, 15, 5, 20),
                new DistributedSlice(700, 750, 100, 0)
        );

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 110), 0, childId, streamId1),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(0)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 130), 0, childId, streamId2),
                        new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(0)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 120, 270), 0, childId, streamId1),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(1)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 510), 0, childId, streamId1),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(2)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 460, 690), 0, childId, streamId2),
                        new HolisticWindowAggregate(Arrays.asList(stream1Slices.get(1)))),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 550, 660), 0, childId, streamId1),
                        new HolisticWindowAggregate(Arrays.asList(stream0Slices.get(3))))
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow).collect(Collectors.toList());

        List<Matcher<? super List<String>>> sessionStartMatchers = Arrays.asList(
                equalsSessionStart(FunctionWindowAggregateId.sessionStartId(0, 120, childId, 0)),
                equalsSessionStart(FunctionWindowAggregateId.sessionStartId(0, 400, childId, 0)),
                equalsSessionStart(FunctionWindowAggregateId.sessionStartId(0, 550, childId, 0)),
                equalsSessionStart(FunctionWindowAggregateId.sessionStartId(0, 700, childId, 1))
        );

        List<Matcher<? super List<String>>> allMatchers = new ArrayList<>(windowMatchers);
        allMatchers.addAll(sessionStartMatchers);

        List<List<String>> windowStrings = receiveWindows(allMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(allMatchers));

        assertChildEnd();
    }

    @Test
    void testSingleStreamCountWindow() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,3,0,COUNT", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 1);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events = { "0,10,1", "0,20,2", "0,30,3", "0,40,4", "0,50,5", "0,60,6", "0,70,7" };
        sendSortedEvents(streamSenders, events);

        List<Matcher<? super List<String>>> expectedEvents = Arrays.asList(
                EventMatcher.equalsEvent("0,10,1"),
                EventMatcher.equalsEvent("0,20,2"),
                EventMatcher.equalsEvent("0,30,3"),
                EventMatcher.equalsEvent("0,40,4"),
                EventMatcher.equalsEvent("0,50,5"),
                EventMatcher.equalsEvent("0,60,6"),
                EventMatcher.equalsEvent("0,70,7")
        );

        List<List<String>> actualEventStrings = new ArrayList<>();
        for (int i = 0; i < expectedEvents.size(); i++) {
            actualEventStrings.add(rootWindowReceiver.receiveNext(2));
        }

        assertThat(actualEventStrings, contains(expectedEvents));

        assertChildEnd();
    }

    @Test
    void testTwoStreamsCountAndTimeWindow() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("30", START_TIME, "TUMBLING,3,0,COUNT" + ARG_DELIMITER + "TUMBLING,30,1", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        String[] events0 = { "0,10,1", "0,20,2", "0,30,3", "0,40,4", "0,50,5", "0,60,6", "0,70,7" };
        String[] events1 = { "1,15,10", "1,25,20", "1,35,30", "1,45,40", "1,55,50", "1,65,60", "1,75,70" };

        sendSleepSortedEvents(10, streamSenders, events0, events1);

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(0, defaultFunctionWindowId(new WindowAggregateId(1,  0, 30)), new DistributiveWindowAggregate(33)),
                new ExpectedWindow(0, defaultFunctionWindowId(new WindowAggregateId(1, 30, 60)), new DistributiveWindowAggregate(132)),
                new ExpectedWindow(0, defaultFunctionWindowId(new WindowAggregateId(1, 60, 90)), new DistributiveWindowAggregate(143))
        );

        List<Matcher<? super List<String>>> expectedMessagesToRoot = Arrays.asList(
                EventMatcher.equalsEvent("0,10,1"),
                EventMatcher.equalsEvent("1,15,10"),
                EventMatcher.equalsEvent("0,20,2"),
                EventMatcher.equalsEvent("1,25,20"),
                EventMatcher.equalsEvent("0,30,3"),
                equalsWindow(expectedWindows.get(0)),
                EventMatcher.equalsEvent("1,35,30"),
                EventMatcher.equalsEvent("0,40,4"),
                EventMatcher.equalsEvent("1,45,40"),
                EventMatcher.equalsEvent("0,50,5"),
                EventMatcher.equalsEvent("1,55,50"),
                EventMatcher.equalsEvent("0,60,6"),
                equalsWindow(expectedWindows.get(1)),
                EventMatcher.equalsEvent("1,65,60"),
                EventMatcher.equalsEvent("0,70,7"),
                EventMatcher.equalsEvent("1,75,70"),
                equalsWindow(expectedWindows.get(2))
        );

        List<List<String>> actualEventStrings = receiveWindows(expectedMessagesToRoot.size(), rootWindowReceiver);
        assertThat(actualEventStrings, containsInAnyOrder(expectedMessagesToRoot));

        assertChildEnd();
    }

    @Test
    void testTwoStreamsMixedKeysSum() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", START_TIME, "TUMBLING,100,0", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        int key0 = 0;
        int key1 = 1;
        int key2 = 2;

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,1", "0,90,1,0",  // window 1
                "0,110,10,1", "0,115,20,1", "0,120,30,1", "0,190,40,2", "0,195,50,2",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,0", "1,60,1,0", "1,80,1,0", "1,85,1,1",  // window 1
                "1,175,10,1", "1,180,20,0", "1,185,30,1", "1,190,40,0", "1,195,50,1",  // window 2
        };

        sendSleepSortedEvents(50, streamSenders, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        Long dummyNull = null;

        List<ExpectedWindow> expectedWindows = Arrays.asList(
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0, childId),
                        new DistributiveWindowAggregate(  7, key0),
                        new DistributiveWindowAggregate(  3, key1),
                        new DistributiveWindowAggregate(dummyNull, key2)),
                new ExpectedWindow(childId, new FunctionWindowAggregateId(new WindowAggregateId(0, 100, 200), 0, childId),
                        new DistributiveWindowAggregate( 60, key0),
                        new DistributiveWindowAggregate(150, key1),
                        new DistributiveWindowAggregate( 90, key2))
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(WindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), rootWindowReceiver);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
    }
}
