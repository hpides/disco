package com.github.lawben.disco;

import de.tub.dima.scotty.core.WindowAggregateId;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ResultListener implements Runnable {
    String resultPath;

    public ResultListener(String resultPath) {
        this.resultPath = resultPath;
    }

    @Override
    public void run() {
        System.out.println(this.resultString("Starting on path " + this.resultPath));

        try (ZContext context = new ZContext()) {
            ZMQ.Socket resultListener = context.createSocket(SocketType.PULL);
            resultListener.bind(DistributedUtils.buildIpcUrl(this.resultPath));

            while (!Thread.currentThread().isInterrupted()) {
                final String rawAggIdOrStreamEnd = resultListener.recvStr();
                if (rawAggIdOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                    System.out.println(this.resultString("Received stream end. Shutting down..."));
                    return;
                }

                final byte[] rawAggregatedResult = resultListener.recv(ZMQ.DONTWAIT);

                final WindowAggregateId windowId = DistributedUtils.stringToWindowId(rawAggIdOrStreamEnd);
                final Object aggregateObject = DistributedUtils.bytesToObject(rawAggregatedResult);
                final Integer finalAggregate = (Integer) aggregateObject;
                System.out.println(this.resultString("FINAL WINDOW: " + windowId + " --> " + finalAggregate));
            }
        }
    }

    private String resultString(String msg) {
        return "[RESULT] " + msg;
    }
}
