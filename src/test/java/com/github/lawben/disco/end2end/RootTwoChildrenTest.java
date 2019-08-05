package com.github.lawben.disco.end2end;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.utils.TestUtils.closeIfNotNull;
import static com.github.lawben.disco.utils.TestUtils.findChildPort;
import static com.github.lawben.disco.utils.TestUtils.receiveWindows;
import static com.github.lawben.disco.utils.WindowResultMatcher.equalsWindowResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedRoot;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import com.github.lawben.disco.utils.ZMQRequestMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.Utils;

public class RootTwoChildrenTest {
    private int controllerPort;
    private int windowPort;
    private String resultPath;
    private String rootIp;

    private DistributedRoot root;
    private DistributedChild child0;
    private DistributedChild child1;
    private ZMQPushMock stream0;
    private ZMQPushMock stream1;

    private ZMQPullMock resultListener;

    @BeforeEach
    public void setup() throws IOException {
        this.controllerPort = Utils.findOpenPort();
        this.windowPort = Utils.findOpenPort();
        this.rootIp = "localhost";

        File tempFile = File.createTempFile("disco-test-", "");
        tempFile.deleteOnExit();
        this.resultPath = tempFile.getAbsolutePath();

        this.resultListener = new ZMQPullMock(this.resultPath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeIfNotNull(this.resultListener);
    }

    Thread runThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }

    void startNodes(List<String> windows, List<String> aggFns) throws Exception {
        int numChildren = 2;
        String windowsString = String.join(";", windows);
        String aggFnsString = String.join(";", aggFns);
        root = new DistributedRoot(controllerPort, windowPort, resultPath, numChildren, windowsString, aggFnsString);

        runThread(root);

        int child0Port = findChildPort();
        int child0Id = 0;
        child0 = new DistributedChild(rootIp, controllerPort, windowPort, child0Port, child0Id, 1);

        int child1Port = findChildPort();
        int child1Id = 1;
        child1 = new DistributedChild(rootIp, controllerPort, windowPort, child1Port, child1Id, 1);

        runThread(child0);
        runThread(child1);

        stream0 = registerStream(0, child0Port);
        stream1 = registerStream(1, child1Port);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
    }

    ZMQPushMock registerStream(int streamId, int childPort) {
        String streamIdString = String.valueOf(streamId);
        try (ZMQRequestMock streamRegister = new ZMQRequestMock(childPort + STREAM_REGISTER_PORT_OFFSET)) {
            streamRegister.addMessage(streamIdString);

            List<String> registerResult = streamRegister.requestNext();
            assertThat(registerResult, not(empty()));
            assertThat(registerResult.get(0), hasLength(1));
            assertEquals('\0', registerResult.get(0).charAt(0));
        }

        return new ZMQPushMock(childPort);
    }

    void sendSleepSortedEvents(int sleep, String[]... events) throws Exception {
        List<ZMQPushMock> streamSenders = Arrays.asList(this.stream0, this.stream1);
        List<String> sortedEvents = Stream.of(events)
                .flatMap(Arrays::stream)
                .sorted(Comparator.comparingInt((String e) -> Integer.parseInt(e.split(",")[1])))
                .collect(Collectors.toList());

        for (String event : sortedEvents) {
            int streamId = Character.getNumericValue(event.charAt(0));
            streamSenders.get(streamId).sendNext(event);
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }

        for (int streamId = 0; streamId < streamSenders.size(); streamId++) {
            streamSenders.get(streamId).sendNext(STREAM_END, String.valueOf(streamId));
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }
    }

    void sendSortedEvents(String[]... events) throws Exception {
        sendSleepSortedEvents(0, events);
    }

    void assertRootEnd() {
        String rootEnd = resultListener.receiveNext().get(0);
        assertThat(rootEnd, equalTo(STREAM_END));
    }

    @Test
    void testTumblingSum() throws Exception {
        List<String> windows = Arrays.asList("TUMBLING,100,0");
        List<String> aggFns = Arrays.asList("SUM");
        startNodes(windows, aggFns);

        String[] events0 = { "0,10,1,0", "0,20,1,1", "0,30,1,0", "0,40,1,1", "0,50,1,0" };
        String[] events1 = { "1,10,1,1", "1,20,1,0", "1,30,1,1", "1,40,1,0", "1,50,1,1" };

        sendSortedEvents(events0, events1);

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0), 5),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0), 5)
        );

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), resultListener);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));
        assertRootEnd();
    }

    @Test
    void testSessionMedian() throws Exception {
        List<String> windows = Arrays.asList("SESSION,100,0");
        List<String> aggFns = Arrays.asList("MEDIAN");
        startNodes(windows, aggFns);

        String[] events0 = {
                "0,0,1", "0,2,2", "0,4,3", "0,5,4", "0,7,5", "0,10,6",
                "0,120,0", "0,140,5", "0,170,10",
                "0,400,0", "0,405,5", "0,410,15",
                "0,550,100", "0,560,0",
                "0,730,0", "0,930,0" // needed to end previous window
        };

        String[] events1 = {
                "1,0,1", "1,15,2", "1,30,3",
                "1,460,15", "1,530,5", "1,590,20",
                "1,700,0", "1,750,0",
                "1,900,0" // needed to end previous window
        };

        sendSleepSortedEvents(100, events0, events1);

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 270), 0),  3),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 690), 0), 15)
        );

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), resultListener);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));
        assertRootEnd();
    }
}
