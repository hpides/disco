import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.lawben.disco.DistributedChild;
import com.github.lawben.disco.DistributedUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.Utils;
import utils.ZMQMock;
import utils.ZMQPullMock;
import utils.ZMQRequestMock;
import utils.ZMQRespondMock;
import utils.ZMQPushMock;

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
        this.childPort = Utils.findOpenPort();
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

    void assertNoThreadException(DistributedChild child) throws InterruptedException {
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        child.interrupt();
        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);
        assertNull(threadException);
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
            assertEquals(registerResult.get(0).charAt(0), '\0');
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

        assertNoThreadException(child);
    }

    @Test
    void testRegisterChildMultiWindowMultiAggFn() throws Exception {
        int rootInitPort = Utils.findOpenPort();
        rootRegisterResponder = new ZMQRespondMock(rootInitPort);
        rootRegisterResponder.addMessage("100", "TUMBLING,100,0\nSLIDING,100,50,1\nSESSION,100,2", "SUM\nAVG\nMEDIAN");

        DistributedChild child = runDefaultChild(rootInitPort, 0);

        List<String> initMsg = rootRegisterResponder.respondToNext();
        assertThat(initMsg, not(empty()));
        assertThat(initMsg.get(0), containsString("new child"));

        assertNoThreadException(child);
    }

    @Test
    void testOneStreamRegister() throws Exception {
        DistributedChild child = defaultInit();
        registerStream(0);
        assertNoThreadException(child);
    }

    @Test
    void testTwoStreamsRegister() throws Exception {
        DistributedChild child = defaultInit(2);
        registerStream(0);
        registerStream(1);
        assertNoThreadException(child);
    }

    @Test
    void testSingleEventSingleStream() throws Exception {
        DistributedChild child = defaultInit();
        registerStream(0);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        ZMQPushMock streamSender = streamSenders.get(0);

        String event = "0,1,1";
        streamSender.addMessage(event);
        streamSender.sendNext();

        streamSender.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> windowString = rootWindowReceiver.receiveNext(3);
        assertThat(windowString, hasSize(3));
        assertEquals(windowString.get(0), "0");
        assertEquals(windowString.get(1), "0,0,100");
        assertEquals(windowString.get(2), "1");

        List<String> childEnd = rootWindowReceiver.receiveNext(2);
        assertThat(childEnd, hasSize(2));
        assertEquals(childEnd.get(0), DistributedUtils.STREAM_END);
        assertEquals(childEnd.get(1), String.valueOf(childId));

        assertNoThreadException(child);
    }

    @Test
    void testFiveEventsSingleStream() throws Exception {
        DistributedChild child = defaultInit();
        registerStream(0);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

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
        assertThat(windowString, hasSize(3));
        assertEquals(windowString.get(0), "0");
        assertEquals(windowString.get(1), "0,0,100");
        assertEquals(windowString.get(2), "5");

        List<String> childEnd = rootWindowReceiver.receiveNext(2);
        assertThat(childEnd, hasSize(2));
        assertEquals(childEnd.get(0), DistributedUtils.STREAM_END);
        assertEquals(childEnd.get(1), String.valueOf(childId));

        assertNoThreadException(child);
    }

    @Test
    void testTwoWindowsSingleStream() throws Exception {
        DistributedChild child = defaultInit();
        registerStream(0);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

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
        assertThat(window1String, hasSize(3));
        assertEquals(window1String.get(0), "0");
        assertEquals(window1String.get(1), "0,0,100");
        assertEquals(window1String.get(2), "5");

        List<String> window2String = rootWindowReceiver.receiveNext(3);
        assertThat(window2String, hasSize(3));
        assertEquals(window2String.get(0), "0");
        assertEquals(window2String.get(1), "0,100,200");
        assertEquals(window2String.get(2), "150");

        List<String> childEnd = rootWindowReceiver.receiveNext(2);
        assertThat(childEnd, hasSize(2));
        assertEquals(childEnd.get(0), DistributedUtils.STREAM_END);
        assertEquals(childEnd.get(1), String.valueOf(childId));

        assertNoThreadException(child);
    }

    @Test
    void testTwoWindowsTwoStreams() throws Exception {
        DistributedChild child = defaultInit(2);
        registerStream(0);
        registerStream(1);

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        ZMQPushMock streamSender0 = streamSenders.get(0);
        ZMQPushMock streamSender1 = streamSenders.get(1);

        String[] events0 = {
                "0,10,1", "0,20,1", "0,30,1", "0,40,1", "0,50,1",  // window 1
                "0,110,10", "0,120,20", "0,130,30", "0,140,40", "0,150,50",  // window 2
        };
        for (String event : events0) {
            streamSender0.addMessage(event);
            streamSender0.sendNext();
        }

        String[] events1 = {
                "1,10,1", "1,20,1", "1,30,1", "1,40,1", "1,50,1",  // window 1
                "1,110,10", "1,120,20", "1,130,30", "1,140,40", "1,150,50",  // window 2
        };
        for (String event : events1) {
            streamSender1.addMessage(event);
            streamSender1.sendNext();
        }

        streamSender0.addMessage(DistributedUtils.STREAM_END, "0");
        streamSender0.sendNext();

        streamSender1.addMessage(DistributedUtils.STREAM_END, "1");
        streamSender1.sendNext();

        Thread.sleep(DEFAULT_SOCKET_TIMEOUT_MS);

        List<String> window1String = rootWindowReceiver.receiveNext(3);
        assertThat(window1String, hasSize(3));
        assertEquals(window1String.get(0), "0");
        assertEquals(window1String.get(1), "0,0,100");
        assertEquals(window1String.get(2), "10");

        List<String> window2String = rootWindowReceiver.receiveNext(3);
        assertThat(window2String, hasSize(3));
        assertEquals(window2String.get(0), "0");
        assertEquals(window2String.get(1), "0,100,200");
        assertEquals(window2String.get(2), "300");

        List<String> childEnd = rootWindowReceiver.receiveNext(2);
        assertThat(childEnd, hasSize(2));
        assertEquals(childEnd.get(0), DistributedUtils.STREAM_END);
        assertEquals(childEnd.get(1), String.valueOf(childId));

        assertNoThreadException(child);
    }
}
