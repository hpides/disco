package com.github.lawben.disco.end2end;

import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static com.github.lawben.disco.aggregation.FunctionWindowAggregateId.NO_CHILD_ID;
import static com.github.lawben.disco.utils.TestUtils.closeIfNotNull;
import static com.github.lawben.disco.utils.TestUtils.findChildPort;
import static com.github.lawben.disco.utils.TestUtils.receiveWindows;
import static com.github.lawben.disco.utils.TestUtils.registerStream;
import static com.github.lawben.disco.utils.TestUtils.runThread;
import static com.github.lawben.disco.utils.TestUtils.sendSleepSortedEvents;
import static com.github.lawben.disco.utils.TestUtils.sendSortedEvents;
import static com.github.lawben.disco.utils.WindowResultMatcher.equalsWindowResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedMergeNode;
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

public class RootTwoMergerFourChildren {
    private String rootIp;
    private int rootControllerPort;
    private int rootWindowPort;
    private String resultPath;

    private String mergerIp;
    private int merger0ControllerPort;
    private int merger0WindowPort;
    private int merger1ControllerPort;
    private int merger1WindowPort;

    private ZMQPullMock resultListener;
    private List<ZMQPushMock> streamSenders;

    @BeforeEach
    public void setup() throws IOException {
        this.rootIp = "localhost";
        this.rootControllerPort = Utils.findOpenPort();
        this.rootWindowPort = Utils.findOpenPort();
        this.mergerIp = "localhost";
        this.merger0ControllerPort = Utils.findOpenPort();
        this.merger0WindowPort = Utils.findOpenPort();
        this.merger1ControllerPort = Utils.findOpenPort();
        this.merger1WindowPort = Utils.findOpenPort();

        File tempFile = File.createTempFile("disco-test-", "");
        tempFile.deleteOnExit();
        this.resultPath = tempFile.getAbsolutePath();

        this.resultListener = new ZMQPullMock(this.resultPath);
        this.streamSenders = new ArrayList<>();
    }

    @AfterEach
    public void tearDown() throws Exception {
        closeIfNotNull(this.resultListener);
        for (ZMQPushMock stream : streamSenders) {
            closeIfNotNull(stream);
        }
    }

    void assertRootEnd() {
        String rootEnd = resultListener.receiveNext().get(0);
        assertThat(rootEnd, equalTo(STREAM_END));
    }

    void startNodes(List<String> windows, List<String> aggFns) throws Exception {
        int numMergers = 2;
        int numChildrenPerMerger = 2;
        String windowsString = String.join(";", windows);
        String aggFnsString = String.join(";", aggFns);
        DistributedRoot root =
                new DistributedRoot(rootControllerPort, rootWindowPort, resultPath, numMergers, windowsString,
                        aggFnsString);

        runThread(root);

        int merger0Id = 0;
        DistributedMergeNode merger0 = new DistributedMergeNode(rootIp, rootControllerPort, rootWindowPort,
                merger0ControllerPort, merger0WindowPort, numChildrenPerMerger, merger0Id);
        runThread(merger0);

        int merger1Id = 1;
        DistributedMergeNode merger1 = new DistributedMergeNode(rootIp, rootControllerPort, rootWindowPort,
                merger1ControllerPort, merger1WindowPort, numChildrenPerMerger, merger1Id);
        runThread(merger1);

        int child0Port = findChildPort();
        int child0Id = 0;
        DistributedChild child0 = new DistributedChild(mergerIp, merger0ControllerPort, merger0WindowPort, child0Port, child0Id, 1);

        int child1Port = findChildPort();
        int child1Id = 1;
        DistributedChild child1 = new DistributedChild(mergerIp, merger0ControllerPort, merger0WindowPort, child1Port, child1Id, 1);

        int child2Port = findChildPort();
        int child2Id = 2;
        DistributedChild child2 = new DistributedChild(mergerIp, merger1ControllerPort, merger1WindowPort, child2Port, child2Id, 1);

        int child3Port = findChildPort();
        int child3Id = 3;
        DistributedChild child3 = new DistributedChild(mergerIp, merger1ControllerPort, merger1WindowPort, child3Port, child3Id, 1);

        runThread(child0);
        runThread(child1);
        runThread(child2);
        runThread(child3);

        ZMQPushMock stream0 = registerStream(0, child0Port);
        ZMQPushMock stream1 = registerStream(1, child1Port);
        ZMQPushMock stream2 = registerStream(2, child2Port);
        ZMQPushMock stream3 = registerStream(3, child3Port);

        this.streamSenders.add(stream0);
        this.streamSenders.add(stream1);
        this.streamSenders.add(stream2);
        this.streamSenders.add(stream3);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
    }

    @Test
    void testTumblingSum() throws Exception {
        List<String> windows = Arrays.asList("TUMBLING,100,0");
        List<String> aggFns = Arrays.asList("SUM");
        startNodes(windows, aggFns);

        String[] events0 = { "0,10,1,0", "0,20,1,1", "0,30,1,0", "0,40,1,1", "0,50,1,0" };
        String[] events1 = { "1,10,1,1", "1,20,1,0", "1,30,1,1", "1,40,1,0", "1,50,1,1" };
        String[] events2 = { "2,10,1,0", "2,20,1,1", "2,30,1,1", "2,40,1,0", "2,50,1,1" };
        String[] events3 = { "3,10,1,1", "3,20,1,0", "3,30,1,1", "3,40,1,0", "3,50,1,2" };

        sendSortedEvents(streamSenders, events0, events1, events2, events3);

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0, NO_CHILD_ID, 0),  9),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0, NO_CHILD_ID, 1), 10),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 0, 100), 0, NO_CHILD_ID, 2),  1)
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
                "0,730,1",
                "0,930,0" // needed to end previous window
        };

        String[] events1 = {
                "1,0,1", "1,15,2", "1,30,3",
                "1,460,15", "1,530,5", "1,590,20",
                "1,700,2", "1,750,3",
                "1,900,0" // needed to end previous window
        };

        String[] events2 = {
                 "2,50,10",  "2,75,15",
                "2,150,10", "2,175,15",
                "2,450,10", "2,475,15",
                "2,750,10",
                "2,950,10" // needed to end previous window
        };

        String[] events3 = {
                 "3,40,10",  "3,65,15",
                "3,140,10", "3,165,15",
                "3,440,10", "3,465,15",
                "3,745,15",
                "3,950,10" // needed to end previous window
        };

        sendSleepSortedEvents(100, streamSenders, events0, events1, events2, events3);

        List<Matcher<? super List<String>>> windowMatchers = Arrays.asList(
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0,   0, 275), 0),  6),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 400, 690), 0), 15),
                equalsWindowResult(new FunctionWindowAggregateId(new WindowAggregateId(0, 700, 850), 0),  3)
        );

        List<List<String>> windowStrings = receiveWindows(windowMatchers.size(), resultListener);
        assertThat(windowStrings, containsInAnyOrder(windowMatchers));
        assertRootEnd();
    }
}
