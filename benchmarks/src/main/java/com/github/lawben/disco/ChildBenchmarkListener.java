package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class ChildBenchmarkListener implements Runnable {
    private final DistributedNode nodeImpl;

    public ChildBenchmarkListener(int streamInputPort) {
        this(streamInputPort, 1);
    }

    public ChildBenchmarkListener(int streamInputPort, int numStreams) {
        this.nodeImpl = new DistributedNode(0, "BM_CHILD_LISTENER", streamInputPort + STREAM_REGISTER_PORT_OFFSET,
                streamInputPort, numStreams, "", 0, 0);
        nodeImpl.createDataPuller();
    }

    @Override
    public void run() {
        try {
            this.registerStreams();
            this.processStreams();
        } finally {
            nodeImpl.close();
        }
    }

    private void processStreams() {
        ZMQ.Socket streamInput = nodeImpl.dataPuller;
        System.out.println(nodeImpl.nodeString("Waiting for stream data."));

        long totalLatency = 0;
        long numEvents = 0;

        long lastSecondLatency = 0;
        long lastSecondNumEvents = 0;

        long lastSecondReceiveTime = 0;

        long lastSecondStart = System.currentTimeMillis();
        long lastSecondEnd = lastSecondStart + 1000;

        while (!nodeImpl.isInterrupted()) {
            final long receivingStart = System.nanoTime();
            String eventOrStreamEnd = streamInput.recvStr();
            final long receivingEnd = System.nanoTime();
            if (eventOrStreamEnd == null) {
                continue;
            }

            lastSecondReceiveTime += (receivingEnd - receivingStart);

            if (eventOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                if (nodeImpl.isTotalStreamEnd()) {
                    return;
                }
                continue;
            }

            final Event event = Event.fromString(eventOrStreamEnd);
            final long currentTime = System.currentTimeMillis();
            final long latency = currentTime - event.getValue();
            lastSecondLatency += latency;
            lastSecondNumEvents++;

            if (currentTime < lastSecondEnd) {
                continue;
            }

            totalLatency += lastSecondLatency;
            numEvents += lastSecondNumEvents;
            final long lastSecondAvgLatency = lastSecondLatency / lastSecondNumEvents;
            final long lastSecondAvgReceive = lastSecondReceiveTime / lastSecondNumEvents;
            final long totalAvgLatency = totalLatency / numEvents;
            System.out.println("Avg. receive: " + lastSecondAvgReceive);
            System.out.println("Current avg. latency: " + lastSecondAvgLatency);
            System.out.println("Total avg. latency: " + totalAvgLatency);

            lastSecondLatency = 0;
            lastSecondNumEvents = 0;
            lastSecondReceiveTime = 0;
            lastSecondEnd = currentTime + 1000;
        }
    }

    private void registerStreams() {
        final ZMQ.Socket streamReceiver = nodeImpl.context.createSocket(SocketType.REP);
        streamReceiver.setReceiveTimeOut(DEFAULT_SOCKET_TIMEOUT_MS);
        streamReceiver.bind(DistributedUtils.buildBindingTcpUrl(nodeImpl.dataPort + STREAM_REGISTER_PORT_OFFSET));

        byte[] ackResponse = new byte[] {'\0'};

        int numRegisteredStreams = 0;
        while (!nodeImpl.isInterrupted()) {
            final String rawStreamId = streamReceiver.recvStr();

            if (rawStreamId == null) {
                continue;
            }

            streamReceiver.send(ackResponse);
            numRegisteredStreams++;

            if (numRegisteredStreams == nodeImpl.numChildren) {
                // All streams registered
                System.out.println(nodeImpl.nodeString("Registered all streams (" + numRegisteredStreams + " in total)"));
                return;
            }
        }
    }
}
