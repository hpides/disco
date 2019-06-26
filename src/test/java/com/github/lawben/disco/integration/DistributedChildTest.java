package com.github.lawben.disco.integration;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedUtils;
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
import java.util.Collection;
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

    private Throwable threadException;

    private ZMQRespondMock rootRegisterResponder;
    private ZMQPullMock rootWindowReceiver;
    private List<ZMQPushMock> streamSenders;

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

    void assertWindowEquals(List<String> rawWindowString, WindowAggregateId windowId, Integer value) {
        assertThat(rawWindowString, hasSize(3));
        assertEquals(String.valueOf(this.childId), rawWindowString.get(0));
        String expectedWindowString = windowId.getWindowId() + "," + windowId.getWindowStartTimestamp() +
                "," + windowId.getWindowEndTimestamp();
        assertEquals(expectedWindowString, rawWindowString.get(1));
        assertEquals(String.valueOf(value), rawWindowString.get(2));
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

    void registerStream(int streamId) {
        String streamIdString = String.valueOf(streamId);
        try (ZMQRequestMock streamRegister = new ZMQRequestMock(childPort + STREAM_REGISTER_PORT_OFFSET)) {
            streamRegister.addMessage(streamIdString);

            List<String> registerResult = streamRegister.requestNext();
            assertThat(registerResult, not(empty()));
            assertThat(registerResult.get(0), hasLength(1));
            assertEquals('\0', registerResult.get(0).charAt(0));
        }

        streamSenders.add(new ZMQPushMock(childPort));
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

        List<String> windowString = rootWindowReceiver.receiveNext(3);
        assertWindowEquals(windowString, new WindowAggregateId(0, 0, 100), 1);

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

        List<String> windowString = rootWindowReceiver.receiveNext(3);
        assertWindowEquals(windowString, new WindowAggregateId(0, 0, 100), 5);

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

        List<String> window1String = rootWindowReceiver.receiveNext(3);
        assertWindowEquals(window1String, new WindowAggregateId(0, 0, 100), 5);

        List<String> window2String = rootWindowReceiver.receiveNext(3);
        assertWindowEquals(window2String, new WindowAggregateId(0, 100, 200), 150);

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

        List<String> window1String = rootWindowReceiver.receiveNext(3);
        assertWindowEquals(window1String, new WindowAggregateId(0, 0, 100), 10);

        List<String> window2String = rootWindowReceiver.receiveNext(3);
        assertWindowEquals(window2String, new WindowAggregateId(0, 100, 200), 300);

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

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(0,   0, 100),  10, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(1,   0, 100),  10, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(2,  10, 180),  70, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(1,  50, 150),  66, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(0, 100, 200), 300, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(1, 100, 200), 300, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(1, 150, 250), 240, childId))
        );

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(rootWindowReceiver.receiveNext(3));
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

        ZMQPushMock streamSender0 = streamSenders.get(0);
        ZMQPushMock streamSender1 = streamSenders.get(1);

        String[] events0 = {
                "0,10,1", "0,30,1", "0,50,1", "0,70,1", "0,90,1",  // window 1
                "0,110,10", "0,115,20", "0,120,30", "0,190,40", "0,195,50",  // window 2
        };

        String[] events1 = {
                "1,20,1", "1,40,1", "1,60,1", "1,80,1", "1,85,1",  // window 1
                "1,175,10", "1,180,20", "1,185,30", "1,190,40", "1,195,50",  // window 2
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

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(0,   0, 100),  1, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(0,  50, 150),  7, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(0, 100, 200), 30, childId)),
            WindowMatcher.equalsWindow(new ExpectedWindow(new WindowAggregateId(0, 150, 250), 34, childId))
        );

        List<List<String>> windowStrings = new ArrayList<>(windowMatchers.size());
        for (int i = 0; i < windowMatchers.size(); i++) {
            windowStrings.add(rootWindowReceiver.receiveNext(3));
        }

        assertThat(windowStrings, containsInAnyOrder(windowMatchers));

        assertChildEnd();
        assertNoFinalThreadException(child);
    }
}
