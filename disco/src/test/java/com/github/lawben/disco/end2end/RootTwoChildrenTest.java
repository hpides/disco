package com.github.lawben.disco.end2end;

import static com.github.lawben.disco.DistributedUtils.ARG_DELIMITER;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.utils.TestUtils.closeIfNotNull;
import static com.github.lawben.disco.utils.TestUtils.findChildPort;
import static com.github.lawben.disco.utils.TestUtils.receiveResultWindows;
import static com.github.lawben.disco.utils.TestUtils.registerStream;
import static com.github.lawben.disco.utils.TestUtils.runThread;
import static com.github.lawben.disco.utils.TestUtils.sendSleepSortedEvents;
import static com.github.lawben.disco.utils.TestUtils.sendSortedEvents;
import static com.github.lawben.disco.utils.WindowResultMatcher.equalsWindowResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedRoot;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.utils.ZMQPullMock;
import com.github.lawben.disco.utils.ZMQPushMock;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private List<ZMQPushMock> streamSenders;

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
        this.streamSenders = new ArrayList<>();
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeIfNotNull(this.resultListener);
    }

    void startNodes(List<String> windows, List<String> aggFns) throws Exception {
        int numChildren = 2;
        String windowsString = String.join(ARG_DELIMITER, windows);
        String aggFnsString = String.join(ARG_DELIMITER, aggFns);
        DistributedRoot root =
                new DistributedRoot(controllerPort, windowPort, resultPath, numChildren, windowsString, aggFnsString);

        runThread(root);

        int child0Port = findChildPort();
        int child0Id = 0;
        DistributedChild child0 = new DistributedChild(rootIp, controllerPort, windowPort, child0Port, child0Id, 1);

        int child1Port = findChildPort();
        int child1Id = 1;
        DistributedChild child1 = new DistributedChild(rootIp, controllerPort, windowPort, child1Port, child1Id, 1);

        runThread(child0);
        runThread(child1);

        ZMQPushMock stream0 = registerStream(0, child0Port);
        ZMQPushMock stream1 = registerStream(1, child1Port);

        this.streamSenders.add(stream0);
        this.streamSenders.add(stream1);
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
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

        sendSortedEvents(streamSenders, events0, events1);

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0), 5L),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0), 5L)
        );

        List<List<String>> windowStrings = receiveResultWindows(windowMatchers.size(), resultListener);
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
                "0,730,3",
                "0,930,0" // needed to end previous window
        };

        String[] events1 = {
                "1,0,1", "1,15,2", "1,30,3",
                "1,460,15", "1,530,5", "1,590,20",
                "1,700,2", "1,750,1",
                "1,900,0" // needed to end previous window
        };

        sendSleepSortedEvents(100, streamSenders, events0, events1);

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 270), 0),  3L),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 690), 0), 15L),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 700, 850), 0),  2L)
        );

        List<List<String>> windowStrings = receiveResultWindows(windowMatchers.size(), resultListener);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));
        assertRootEnd();
    }
}
