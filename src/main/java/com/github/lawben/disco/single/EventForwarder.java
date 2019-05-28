package com.github.lawben.disco.single;

import com.github.lawben.disco.DistributedUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class EventForwarder implements Runnable {

    private final String rootIp;
    private final int rootPort;
    private final int streamInputPort;
    private final int childId;

    private final static int TIMEOUT_MS = 10 * 1000;

    public EventForwarder(String rootIp, int rootPort, int streamInputPort, int childId) {
        this.rootIp = rootIp;
        this.rootPort = rootPort;
        this.streamInputPort = streamInputPort;
        this.childId = childId;
    }

    @Override
    public void run() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket streamInput = context.createSocket(SocketType.PULL);
            streamInput.bind(DistributedUtils.buildBindingTcpUrl(this.streamInputPort));

            ZMQ.Socket streamOutput = context.createSocket(SocketType.PUSH);
            streamOutput.connect(DistributedUtils.buildTcpUrl(this.rootIp, this.rootPort));

            ZMQ.Poller inputPoller = context.createPoller(1);
            inputPoller.register(streamInput);

            System.out.println(this.forwardIdString("Starting proxy"));

            while (!Thread.currentThread().isInterrupted()) {
                // Receive
                String streamId = streamInput.recvStr();
                if (streamId == null) {
                    assert streamInput.errno() == ZMQ.Error.EAGAIN.getCode();
                    // Timed out --> quit
                    System.out.println(this.forwardIdString("No more data. Shutting down..."));
                    return;
                }

                String eventTimestamp = streamInput.recvStr();
                byte[] eventValueBytes = streamInput.recv();

                // Forward
                streamOutput.sendMore(streamId);
                streamOutput.sendMore(eventTimestamp);
                streamOutput.send(eventValueBytes);
            }

        }
    }

    protected String forwardIdString(String msg) {
        return "[FW-" + this.childId + "] " + msg;
    }
}
