package com.github.lawben.disco.integration;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.DistributedUtils.WINDOW_COMPLETE;
import static com.github.lawben.disco.DistributedUtils.WINDOW_PARTIAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.PartialAverage;
import com.github.lawben.disco.utils.AlgebraicWindowMatcher;
import com.github.lawben.disco.utils.BaseWindowMatcher;
import com.github.lawben.disco.utils.DistributiveWindowMatcher;
import com.github.lawben.disco.utils.EventMatcher;
import com.github.lawben.disco.utils.ExpectedAlgebraicWindow;
import com.github.lawben.disco.utils.ExpectedDistributiveWindow;
import com.github.lawben.disco.utils.ExpectedHolisticWindow;
import com.github.lawben.disco.utils.HolisticWindowMatcher;
import com.github.lawben.disco.utils.ZMQMock;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import com.github.lawben.disco.utils.ZMQRequestMock;
import com.github.lawben.disco.utils.ZMQRespondMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.Utils;

public class DistributedChildTest {
    private int childPort;
    private int childId;
    private String rootIp;

    private ZMQRespondMock rootRegisterResponder;
    private ZMQPullMock rootWindowReceiver;
    private List<ZMQPushMock> streamSenders;

    private Throwable threadException;
    private Thread.UncaughtExceptionHandler threadExceptionHandler = (th, ex) -> threadException = ex;

    @BeforeEach
    public void setup() throws IOException {
        while ((this.childPort = Utils.findOpenPort()) > 65536 - 100) {
            this.childPort = Utils.findOpenPort();
        }
        this.childId = 0;
        this.rootIp = "localhost";
        this.threadException = null;
        this.rootRegisterResponder = null;
        this.rootWindowReceiver = null;
        this.streamSenders = new ArrayList<>();
    }

    @AfterEach
    public void tearDown() throws Exception {
        this.threadException = null;
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

    void runThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setUncaughtExceptionHandler(threadExceptionHandler);
        thread.start();
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

    void assertNoFinalThreadException(DistributedChild child) throws InterruptedException {
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        child.interrupt();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);
    }

    void assertDistributiveWindowEquals(List<String> rawWindowString, FunctionWindowAggregateId functionWindowId, Integer value) {
        assertThat(rawWindowString, hasSize(5));
        assertEquals(String.valueOf(this.childId), rawWindowString.get(0));

        assertTrue(BaseWindowMatcher.functionWindowIdStringsMatch(functionWindowId, rawWindowString.get(1)));

        assertThat(rawWindowString.get(2), anyOf(equalTo(WINDOW_COMPLETE), equalTo(WINDOW_PARTIAL)));

        assertEquals(DistributedUtils.DISTRIBUTIVE_STRING, rawWindowString.get(3));
        assertEquals(String.valueOf(value), rawWindowString.get(4));
    }

    List<String> receiveWindow(ZMQPullMock receiver) {
        return receiver.receiveNext(5);
    }


    DistributedChild defaultInit() throws Exception {
        return defaultInit(1);
    }

    DistributedChild defaultInit(int numStreams) throws Exception {
        int rootInitPort = Utils.findOpenPort();
        try (ZMQRespondMock rootRegisterResponder = new ZMQRespondMock(rootInitPort)) {
            rootRegisterResponder.addMessage("100", "TUMBLING,100,0", "SUM");

            int rootWindowPort = Utils.findOpenPort();
            rootWindowReceiver = new ZMQPullMock(rootWindowPort);

            DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, numStreams);
            rootRegisterResponder.respondToNext();
            Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
            assertNull(threadException);

            for (int i = 0; i < numStreams; i++) {
                registerStream(i);
            }

            Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
            assertNull(threadException);
            return child;
        }
    }

    ZMQPushMock registerStream(int streamId) {
        String streamIdString = String.valueOf(streamId);
        try (ZMQRequestMock streamRegister = new ZMQRequestMock(childPort + STREAM_REGISTER_PORT_OFFSET)) {
            streamRegister.addMessage(streamIdString);

            List<String> registerResult = streamRegister.requestNext();
            assertThat(registerResult, not(empty()));
            assertThat(registerResult.get(0), hasLength(1));
            assertEquals('\0', registerResult.get(0).charAt(0));
        }

        ZMQPushMock stream = new ZMQPushMock(childPort);
        streamSenders.add(stream);
        return stream;
    }

    void sendSleepSortedEvents(int sleep, String[]... events) throws Exception {
        List<String> sortedEvents = Stream.of(events)
                .flatMap(Arrays::stream)
                .sorted(Comparator.comparingInt((String e) -> Integer.valueOf(e.split(",")[1])))
                .collect(Collectors.toList());

        for (String event : sortedEvents) {
            int streamId = Character.getNumericValue(event.charAt(0));
            this.streamSenders.get(streamId).sendNext(event);
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }

        for (int streamId = 0; streamId < this.streamSenders.size(); streamId++) {
            this.streamSenders.get(streamId).sendNext(STREAM_END, String.valueOf(streamId));
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }
    }

    void sendSortedEvents(String[]... events) throws Exception {
        sendSleepSortedEvents(0, events);
    }

    @Test
    void testRegisterChild() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100", "SUM");

        DistributedChild child = runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new child"));

        assertNoFinalThreadException(child);
    }

    @Test
    void testRegisterChildMultiWindowSum() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100,0\nSLIDING,100,50,1\nSESSION,100,2", "SUM");

        DistributedChild child = runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new child"));

        assertNoFinalThreadException(child);
    }

    @Test
    void testRegisterChildMultiWindowAvg() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100,0\nSLIDING,100,50,1\nSESSION,100,2", "AVG");

        DistributedChild child = runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new child"));

        assertNoFinalThreadException(child);
    }

    @Test
    void testRegisterChildMultiWindowMedian() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100,0\nSLIDING,100,50,1\nSESSION,100,2", "MEDIAN");

        DistributedChild child = runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new child"));

        assertNoFinalThreadException(child);
    }

    @Test
    void testOneStreamRegister() throws Exception {
        DistributedChild child = defaultInit();
        assertNoFinalThreadException(child);
    }

    @Test
    void testTwoStreamsRegister() throws Exception {
        DistributedChild child = defaultInit(2);
        assertNoFinalThreadException(child);
    }

    @Test
    void testSingleEventSingleStream() throws Exception {
        DistributedChild child = defaultInit();
        ZMQPushMock streamSender = streamSenders.get(0);

        String event = "0,1,1";
        streamSender.addMessage(event);
        streamSender.sendNext();

        streamSender.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> windowString = receiveWindow(rootWindowReceiver);
        assertDistributiveWindowEquals(windowString, defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), 1);

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testFiveEventsSingleStream() throws Exception {
        DistributedChild child = defaultInit();
        ZMQPushMock streamSender = streamSenders.get(0);

        String[] events = { "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1" };
        for (String event : events) {
            streamSender.addMessage(event);
            streamSender.sendNext();
        }

        streamSender.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> windowString = receiveWindow(rootWindowReceiver);
        assertDistributiveWindowEquals(windowString, defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), 5);

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testTwoWindowsSingleStream() throws Exception {
        DistributedChild child = defaultInit();
        ZMQPushMock streamSender = streamSenders.get(0);

        String[] events = {
                "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1",  // window 1
                "0,110,10", "0,120,20", "0,130,30", "0,140,40", "0,150,50",  // window 2
        };
        for (String event : events) {
            streamSender.addMessage(event);
            streamSender.sendNext();
        }

        streamSender.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> window1String = receiveWindow(rootWindowReceiver);
        assertDistributiveWindowEquals(window1String, defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), 5);

        List<String> window2String = receiveWindow(rootWindowReceiver);
        assertDistributiveWindowEquals(window2String, defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)), 150);

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testTwoWindowsTwoStreams() throws Exception {
        DistributedChild child = defaultInit(2);
        ZMQPushMock streamSender0 = streamSenders.get(0);
        ZMQPushMock streamSender1 = streamSenders.get(1);

        String[] events0 = {
                "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1",  // window 1
                "0,110,10", "0,120,20", "0,130,30", "0,140,40", "0,150,50",  // window 2
        };

        String[] events1 = {
                "1,10,1", "1,20,1", "1,30,1", "1,40,1", "1,50,1",  // window 1
                "1,110,10", "1,120,20", "1,130,30", "1,140,40", "1,150,50",  // window 2
        };

        List<String> sortedEvents = Stream.of(Arrays.asList(events0), Arrays.asList(events1))
                .flatMap(Collection::stream)
                .sorted(Comparator.comparingInt((String e) -> Integer.valueOf(e.split(",")[1])))
                .collect(Collectors.toList());

        for (String event : sortedEvents) {
            if (event.startsWith("0")) {
                streamSender0.addMessage(event);
                streamSender0.sendNext();
            } else {
                streamSender1.addMessage(event);
                streamSender1.sendNext();
            }
        }

        streamSender0.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender0.sendNext();

        streamSender1.addMessage(DistributedUtils.STREAM_END, "1");
        streamSender1.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> window1String = receiveWindow(rootWindowReceiver);
        assertDistributiveWindowEquals(window1String, defaultFunctionWindowId(new WindowAggregateId(0, 0, 100)), 10);

        List<String> window2String = receiveWindow(rootWindowReceiver);
        assertDistributiveWindowEquals(window2String, defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)), 300);

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testMultiWindowSumTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100,0\nSLIDING,100,50,1\nSESSION,60,2", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        ZMQPushMock streamSender0 = streamSenders.get(0);
        ZMQPushMock streamSender1 = streamSenders.get(1);

        String[] events0 = {
                "0,10,1", "0,30,1", "0,50,1", "0,70,1", "0,90,1",  // window 1
                "0,110,10", "0,115,20", "0,120,30", "0,190,40", "0,195,50",  // window 2
        };

        String[] events1 = {
                "1,20,1", "1,40,1", "1,60,1", "1,80,1", "1,85,1",  // window 1
                "1,185,10", "1,186,20", "1,187,30", "1,190,40", "1,195,50",  // window 2
        };

        List<String> sortedEvents = Stream.of(Arrays.asList(events0), Arrays.asList(events1))
                .flatMap(Collection::stream)
                .sorted(Comparator.comparingInt((String e) -> Integer.valueOf(e.split(",")[1])))
                .collect(Collectors.toList());

        for (String event : sortedEvents) {
            if (event.startsWith("0")) {
                streamSender0.addMessage(event);
                streamSender0.sendNext();
            } else {
                streamSender1.addMessage(event);
                streamSender1.sendNext();
            }
        }

        streamSender0.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender0.sendNext();

        streamSender1.addMessage(DistributedUtils.STREAM_END, "1");
        streamSender1.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<ExpectedDistributiveWindow> expectedWindows = Arrays.asList(
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(0,   0, 100)),  10, childId),
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1,   0, 100)),  10, childId),
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(2,  10, 180)),  70, childId),
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1,  50, 150)),  66, childId),
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)), 300, childId),
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1, 100, 200)), 300, childId),
            new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1, 150, 250)), 240, childId)
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(DistributiveWindowMatcher::equalsWindow)
                .collect(Collectors.toList());

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(receiveWindow(rootWindowReceiver));
        }

        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testAvgTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "SLIDING,100,50,0", "AVG");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        String[] events0 = {
                "0,10,1", "0,30,1", "0,50,1", "0,70,1", "0,90,1",  // window 1
                "0,110,10", "0,115,20", "0,120,30", "0,190,40", "0,195,50",  // window 2
        };

        String[] events1 = {
                "1,20,1", "1,40,1", "1,60,1", "1,80,1", "1,85,1",  // window 1
                "1,175,10", "1,180,20", "1,185,30", "1,190,40", "1,195,50",  // window 2
        };

        sendSortedEvents(events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<ExpectedAlgebraicWindow> expectedWindows = Arrays.asList(
            new ExpectedAlgebraicWindow(defaultFunctionWindowId(new WindowAggregateId(0,   0, 100)), new PartialAverage( 10, 10), childId),
            new ExpectedAlgebraicWindow(defaultFunctionWindowId(new WindowAggregateId(0,  50, 150)), new PartialAverage( 66,  9), childId),
            new ExpectedAlgebraicWindow(defaultFunctionWindowId(new WindowAggregateId(0, 100, 200)), new PartialAverage(300, 10), childId),
            new ExpectedAlgebraicWindow(defaultFunctionWindowId(new WindowAggregateId(0, 150, 250)), new PartialAverage(240,  7), childId)
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(AlgebraicWindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(receiveWindow(rootWindowReceiver));
        }

        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testMedianTwoStreams() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "SLIDING,100,50,0", "MEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        ZMQPushMock streamSender0 = streamSenders.get(0);
        ZMQPushMock streamSender1 = streamSenders.get(1);

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,0", "0,90,1,0",  // window 1
                "0,110,10,0", "0,115,20,0", "0,120,30,0", "0,190,40,0", "0,195,50,0",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,1", "1,60,1,1", "1,80,1,1", "1,85,1,1",  // window 1
                "1,175,10,1", "1,180,20,1", "1,185,30,1", "1,190,40,1", "1,195,50,1",  // window 2
        };

        List<String> sortedEvents = Stream.of(Arrays.asList(events0), Arrays.asList(events1))
                .flatMap(Collection::stream)
                .sorted(Comparator.comparingInt((String e) -> Integer.valueOf(e.split(",")[1])))
                .collect(Collectors.toList());

        for (String event : sortedEvents) {
            if (event.startsWith("0")) {
                streamSender0.addMessage(event);
                streamSender0.sendNext();
            } else {
                streamSender1.addMessage(event);
                streamSender1.sendNext();
            }
        }

        streamSender0.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender0.sendNext();

        streamSender1.addMessage(DistributedUtils.STREAM_END, "1");
        streamSender1.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  30, Arrays.asList(1, 1)),
                new DistributedSlice( 50,  90, Arrays.asList(1, 1, 1)),
                new DistributedSlice(100, 120, Arrays.asList(10, 20, 30)),
                new DistributedSlice(150, 195, Arrays.asList(40, 50))
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  40, Arrays.asList(1, 1)),
                new DistributedSlice( 50,  85, Arrays.asList(1, 1, 1)),
                new DistributedSlice(150, 195, Arrays.asList(10, 20, 30, 40, 50))
        );

        List<ExpectedHolisticWindow> expectedWindows = Arrays.asList(
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 100), 0, childId, 0), Arrays.asList(stream0Slices.get(0), stream0Slices.get(1)), childId, false),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 100), 0, childId, 1), Arrays.asList(stream1Slices.get(0), stream1Slices.get(1)), childId),

                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0,  50, 150), 0, childId, 0), Arrays.asList(stream0Slices.get(2)) , childId, false),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0,  50, 150), 0, childId, 1), Collections.emptyList(), childId),

                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 100, 200), 0, childId, 0), Arrays.asList(stream0Slices.get(3)) , childId, false),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 100, 200), 0, childId, 1), Arrays.asList(stream1Slices.get(2)) , childId),

                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 150, 250), 0, childId, 0), Collections.emptyList(), childId, false),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 150, 250), 0, childId, 1), Collections.emptyList(), childId)
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(HolisticWindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(receiveWindow(rootWindowReceiver));
        }

        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testTwoStreamsSumAvgMedianAggregatesTwice() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100,0", "SUM\nSUM\nAVG\nAVG\nMEDIAN\nMEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        String[] events0 = {
                "0,10,1,0", "0,30,1,0", "0,50,1,0", "0,70,1,0", "0,90,1,0",  // window 1
                "0,110,10,0", "0,115,20,0", "0,120,30,0", "0,190,40,0", "0,195,50,0",  // window 2
        };

        String[] events1 = {
                "1,20,1,1", "1,40,1,1", "1,60,1,1", "1,80,1,1", "1,85,1,1",  // window 1
                "1,175,10,1", "1,180,20,1", "1,185,30,1", "1,190,40,1", "1,195,50,1",  // window 2
        };

        sendSortedEvents(events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  90, Arrays.asList(1, 1, 1, 1, 1)),
                new DistributedSlice(100, 195, Arrays.asList(10, 20, 30, 40, 50))
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  85, Arrays.asList(1, 1, 1, 1, 1)),
                new DistributedSlice(100, 195, Arrays.asList(10, 20, 30, 40, 50))
        );

        WindowAggregateId window1 = new WindowAggregateId(0,   0, 100);
        WindowAggregateId window2 = new WindowAggregateId(0, 100, 200);

        FunctionWindowAggregateId sum1Window1Id = new FunctionWindowAggregateId(window1, 0);
        FunctionWindowAggregateId sum2Window1Id = new FunctionWindowAggregateId(window1, 1);
        FunctionWindowAggregateId avg1Window1Id = new FunctionWindowAggregateId(window1, 2);
        FunctionWindowAggregateId avg2Window1Id = new FunctionWindowAggregateId(window1, 3);
        FunctionWindowAggregateId med11Window1Id = new FunctionWindowAggregateId(window1, 4, childId, 0);
        FunctionWindowAggregateId med12Window1Id = new FunctionWindowAggregateId(window1, 5, childId, 0);
        FunctionWindowAggregateId med21Window1Id = new FunctionWindowAggregateId(window1, 4, childId, 1);
        FunctionWindowAggregateId med22Window1Id = new FunctionWindowAggregateId(window1, 5, childId, 1);

        FunctionWindowAggregateId sum1Window2Id = new FunctionWindowAggregateId(window2, 0);
        FunctionWindowAggregateId sum2Window2Id = new FunctionWindowAggregateId(window2, 1);
        FunctionWindowAggregateId avg1Window2Id = new FunctionWindowAggregateId(window2, 2);
        FunctionWindowAggregateId avg2Window2Id = new FunctionWindowAggregateId(window2, 3);
        FunctionWindowAggregateId med11Window2Id = new FunctionWindowAggregateId(window2, 4, childId, 0);
        FunctionWindowAggregateId med12Window2Id = new FunctionWindowAggregateId(window2, 5, childId, 0);
        FunctionWindowAggregateId med21Window2Id = new FunctionWindowAggregateId(window2, 4, childId, 1);
        FunctionWindowAggregateId med22Window2Id = new FunctionWindowAggregateId(window2, 5, childId, 1);

        ExpectedDistributiveWindow expectedSum1Window1 = new ExpectedDistributiveWindow(sum1Window1Id, 10, childId);
        ExpectedDistributiveWindow expectedSum2Window1 = new ExpectedDistributiveWindow(sum2Window1Id, 10, childId);
        ExpectedAlgebraicWindow expectedAvg1Window1 = new ExpectedAlgebraicWindow(avg1Window1Id, new PartialAverage(10, 10), childId);
        ExpectedAlgebraicWindow expectedAvg2Window1 = new ExpectedAlgebraicWindow(avg2Window1Id, new PartialAverage(10, 10), childId);
        ExpectedHolisticWindow expectedMed11Window1 = new ExpectedHolisticWindow(med11Window1Id, Arrays.asList(stream0Slices.get(0)), 0, false);
        ExpectedHolisticWindow expectedMed12Window1 = new ExpectedHolisticWindow(med12Window1Id, Arrays.asList(), childId, false);
        ExpectedHolisticWindow expectedMed21Window1 = new ExpectedHolisticWindow(med21Window1Id, Arrays.asList(stream1Slices.get(0)), childId);
        ExpectedHolisticWindow expectedMed22Window1 = new ExpectedHolisticWindow(med22Window1Id, Arrays.asList(), childId);

        ExpectedDistributiveWindow expectedSum1Window2 = new ExpectedDistributiveWindow(sum1Window2Id, 300, childId);
        ExpectedDistributiveWindow expectedSum2Window2 = new ExpectedDistributiveWindow(sum2Window2Id, 300, childId);
        ExpectedAlgebraicWindow expectedAvg1Window2 = new ExpectedAlgebraicWindow(avg1Window2Id, new PartialAverage(300, 10), childId);
        ExpectedAlgebraicWindow expectedAvg2Window2 = new ExpectedAlgebraicWindow(avg2Window2Id, new PartialAverage(300, 10), childId);
        ExpectedHolisticWindow expectedMed11Window2 = new ExpectedHolisticWindow(med11Window2Id, Arrays.asList(stream0Slices.get(1)), childId, false);
        ExpectedHolisticWindow expectedMed12Window2 = new ExpectedHolisticWindow(med12Window2Id, Arrays.asList(), childId, false);
        ExpectedHolisticWindow expectedMed21Window2 = new ExpectedHolisticWindow(med21Window2Id, Arrays.asList(stream1Slices.get(1)), childId);
        ExpectedHolisticWindow expectedMed22Window2 = new ExpectedHolisticWindow(med22Window2Id, Arrays.asList(), childId);


        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                DistributiveWindowMatcher.equalsWindow(expectedSum1Window1),
                DistributiveWindowMatcher.equalsWindow(expectedSum2Window1),
                AlgebraicWindowMatcher.equalsWindow(expectedAvg1Window1),
                AlgebraicWindowMatcher.equalsWindow(expectedAvg2Window1),
                HolisticWindowMatcher.equalsWindow(expectedMed11Window1),
                HolisticWindowMatcher.equalsWindow(expectedMed12Window1),
                HolisticWindowMatcher.equalsWindow(expectedMed21Window1),
                HolisticWindowMatcher.equalsWindow(expectedMed22Window1),
                DistributiveWindowMatcher.equalsWindow(expectedSum1Window2),
                DistributiveWindowMatcher.equalsWindow(expectedSum2Window2),
                AlgebraicWindowMatcher.equalsWindow(expectedAvg1Window2),
                AlgebraicWindowMatcher.equalsWindow(expectedAvg2Window2),
                HolisticWindowMatcher.equalsWindow(expectedMed11Window2),
                HolisticWindowMatcher.equalsWindow(expectedMed12Window2),
                HolisticWindowMatcher.equalsWindow(expectedMed21Window2),
                HolisticWindowMatcher.equalsWindow(expectedMed22Window2)
        );

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(receiveWindow(rootWindowReceiver));
        }

        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testTwoStreamsSessionMedian() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "SESSION,100,0", "MEDIAN");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

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

        sendSleepSortedEvents(100, events0, events1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<DistributedSlice> stream0Slices = Arrays.asList(
                new DistributedSlice(  0,  10, Arrays.asList(1, 2, 3, 4, 5, 6)),
                new DistributedSlice(120, 170, Arrays.asList(0, 5, 10)),
                new DistributedSlice(400, 410, Arrays.asList(0, 5, 15)),
                new DistributedSlice(550, 560, Arrays.asList(100, 0))
        );

        List<DistributedSlice> stream1Slices = Arrays.asList(
                new DistributedSlice(  0,  30, Arrays.asList( 1, 2,  3)),
                new DistributedSlice(460, 590, Arrays.asList(15, 5, 20)),
                new DistributedSlice(700, 750, Arrays.asList(100, 0))
        );

        List<ExpectedHolisticWindow> expectedWindows = Arrays.asList(
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 110), 0, childId, streamId1), Arrays.asList(stream0Slices.get(0)), childId),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 130), 0, childId, streamId2), Arrays.asList(stream1Slices.get(0)), childId),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 120, 270), 0, childId, streamId1), Arrays.asList(stream0Slices.get(1)), childId),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 510), 0, childId, streamId1), Arrays.asList(stream0Slices.get(2)), childId),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 460, 690), 0, childId, streamId2), Arrays.asList(stream1Slices.get(1)), childId),
                new ExpectedHolisticWindow(new FunctionWindowAggregateId(new WindowAggregateId(0, 550, 660), 0, childId, streamId1), Arrays.asList(stream0Slices.get(3)), childId)
        );

        List<Matcher<? super List<String>>> windowMatchers = expectedWindows.stream()
                .map(HolisticWindowMatcher::equalsWindow).collect(Collectors.toList());

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(receiveWindow(rootWindowReceiver));
        }

        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }

    @Test
    void testSingleStreamCountWindow() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,3,0,COUNT", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 1);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        String[] events = { "0,10,1", "0,20,2", "0,30,3", "0,40,4", "0,50,5", "0,60,6", "0,70,7" };
        sendSortedEvents(events);

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
        assertNoFinalThreadException(child);
    }

    @Test
    void testTwoStreamsCountAndTimeWindow() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("30", "TUMBLING,3,0,COUNT\nTUMBLING,30,1", "SUM");

        int rootWindowPort = Utils.findOpenPort();
        rootWindowReceiver = new ZMQPullMock(rootWindowPort);

        DistributedChild child = runDefaultChild(rootInitPort, rootWindowPort, 2);
        rootRegisterResponder.respondToNext();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        registerStream(0);
        registerStream(1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);

        String[] events0 = { "0,10,1", "0,20,2", "0,30,3", "0,40,4", "0,50,5", "0,60,6", "0,70,7" };
        String[] events1 = { "1,15,10", "1,25,20", "1,35,30", "1,45,40", "1,55,50", "1,65,60", "1,75,70" };

        sendSleepSortedEvents(10, events0, events1);

        List<ExpectedDistributiveWindow> expectedWindows = Arrays.asList(
                new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1,  0, 30)),  33, 0),
                new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1, 30, 60)), 132, 0),
                new ExpectedDistributiveWindow(defaultFunctionWindowId(new WindowAggregateId(1, 60, 90)), 143, 0)
        );

        List<Matcher<? super List<String>>> expectedMessagesToRoot = Arrays.asList(
                EventMatcher.equalsEvent("0,10,1"),
                EventMatcher.equalsEvent("1,15,10"),
                EventMatcher.equalsEvent("0,20,2"),
                EventMatcher.equalsEvent("1,25,20"),
                EventMatcher.equalsEvent("0,30,3"),
                EventMatcher.equalsEvent("1,35,30"),
                EventMatcher.equalsEvent("0,40,4"),
                EventMatcher.equalsEvent("1,45,40"),
                EventMatcher.equalsEvent("0,50,5"),
                EventMatcher.equalsEvent("1,55,50"),
                EventMatcher.equalsEvent("0,60,6"),
                DistributiveWindowMatcher.equalsWindow(expectedWindows.get(0)),
                EventMatcher.equalsEvent("1,65,60"),
                EventMatcher.equalsEvent("0,70,7"),
                EventMatcher.equalsEvent("1,75,70"),
                DistributiveWindowMatcher.equalsWindow(expectedWindows.get(1)),
                DistributiveWindowMatcher.equalsWindow(expectedWindows.get(2))
        );

        List<List<String>> actualEventStrings = new ArrayList<>();
        for (Matcher m : expectedMessagesToRoot) {
            if (m instanceof EventMatcher) {
                actualEventStrings.add(rootWindowReceiver.receiveNext(2));
            } else {
                actualEventStrings.add(receiveWindow(rootWindowReceiver));
            }
        }

        assertThat(actualEventStrings, contains(expectedMessagesToRoot));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }
}
