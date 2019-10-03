package com.github.lawben.disco.utils;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.CONTROL_STRING;
import static com.github.lawben.disco.DistributedUtils.EVENT_STRING;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.zeromq.Utils;

public class TestUtils {
    private final static long START_TIME = 1570061857;

    public static void closeIfNotNull(AutoCloseable x) throws Exception {
        if (x != null) {
            x.close();
        }
    }

    public static int findChildPort() throws IOException {
        int childPort;
        while ((childPort = Utils.findOpenPort()) > 65536 - STREAM_REGISTER_PORT_OFFSET) {}
        return childPort;
    }

    public static List<String> receiveResult(ZMQPullMock resultListener) {
        return resultListener.receiveNext(2);
    }

    public static List<String> receiveWindow(ZMQPullMock receiver) {
        List<String> received = receiver.receiveNext();
        String controlMsg = received.get(0);
        if (controlMsg.equals(EVENT_STRING) || controlMsg.equals(CONTROL_STRING)) {
            received.add(receiver.receiveNext().get(0));
            return received;
        }

        received.add(receiver.receiveNext().get(0));
        received.add(receiver.receiveNext().get(0));
        if (received.get(2) == null) {
            System.out.println("Cannot get aggregates for: " + received);
            return null;
        }

        for (int i = 0; i < Integer.parseInt(received.get(2)); i++) {
            received.add(receiver.receiveNext().get(0));
        }
        return received;
    }

    public static List<List<String>> receiveResultWindows(int numExpectedWindows, ZMQPullMock resultListener) {
        List<List<String>> windowStrings = new ArrayList<>(numExpectedWindows);
        for (int i = 0; i < numExpectedWindows; i++) {
            windowStrings.add(receiveResult(resultListener));
        }
        System.out.println("Received window strings: " + windowStrings);
        return windowStrings;
    }

    public static List<List<String>> receiveWindows(int numExpectedWindows, ZMQPullMock dataPuller) {
        List<List<String>> windowStrings = new ArrayList<>(numExpectedWindows);
        for (int i = 0; i < numExpectedWindows; i++) {
            windowStrings.add(receiveWindow(dataPuller));
        }
        System.out.println("Received window strings: " + windowStrings);
        return windowStrings;
    }

    public static Thread runThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }

    public static ZMQPushMock registerStream(int streamId, int childPort) {
        String streamIdString = String.valueOf(streamId);
        try (ZMQRequestMock streamRegister = new ZMQRequestMock(childPort + STREAM_REGISTER_PORT_OFFSET)) {
            streamRegister.addMessage(streamIdString);

            List<String> registerResult = streamRegister.requestNext();
            assertThat(registerResult, not(empty()));
            assertThat(Long.parseLong(registerResult.get(0)), is(greaterThan(START_TIME)));
        }

        return new ZMQPushMock(childPort);
    }

    public static void sendSleepSortedEvents(int sleep, List<ZMQPushMock> streamSenders, String[]... events) throws Exception {
        List<String> sortedEvents = Stream.of(events)
                .flatMap(Arrays::stream)
                .sorted(Comparator.comparingInt((String e) -> Integer.parseInt(e.split(",")[1])))
                .collect(Collectors.toList());

        System.out.println("sorted Events:" + sortedEvents);

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

    public static void sendSortedEvents(List<ZMQPushMock> streamSenders, String[]... events) throws Exception {
        sendSleepSortedEvents(0, streamSenders, events);
    }
}
