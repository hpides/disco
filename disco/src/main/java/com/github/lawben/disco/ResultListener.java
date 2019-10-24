package com.github.lawben.disco;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ResultListener implements Runnable {
    private String resultPath;

    public ResultListener(String resultPath) {
        this.resultPath = resultPath;
    }

    @Override
    public void run() {
        System.out.println(this.resultString("Starting on path " + this.resultPath));

        try (ZContext context = new ZContext()) {
            ZMQ.Socket resultListener = context.createSocket(SocketType.PULL);
            resultListener.bind(DistributedUtils.buildIpcUrl(this.resultPath));

            int numResultWindows = 0;

            while (!Thread.currentThread().isInterrupted()) {
                final String rawAggIdOrStreamEnd = resultListener.recvStr();
                if (rawAggIdOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                    System.out.println(this.resultString("Received stream end. Shutting down..."));
                    return;
                }

                final String rawAggregatedResult = resultListener.recvStr(ZMQ.DONTWAIT);

                final FunctionWindowAggregateId functionWindowAggId =
                        DistributedUtils.stringToFunctionWindowAggId(rawAggIdOrStreamEnd);

                 if (rawAggregatedResult == null || rawAggregatedResult.equals("null")) {
//                     System.out.println("Null latency: " + functionWindowAggId);
                     continue;
                 }

                final Long finalAggregate = Long.valueOf(rawAggregatedResult);
                final long currentTime = System.currentTimeMillis();
//                final long windowEnd = functionWindowAggId.getWindowId().getWindowEndTimestamp();
                final long windowEnd = finalAggregate;
                final long windowLatency = currentTime - windowEnd;
                System.out.println("Latency for window: " + functionWindowAggId + " --> " + windowLatency);

                if (++numResultWindows % 1000 == 0) {
                    System.out.println("Received " + numResultWindows + " results.");
                }

                // System.out.println(this.resultString("FINAL WINDOW: " + functionWindowAggId + " --> " + finalAggregate));

            }
        }
    }

    private String resultString(String msg) {
        return "[RESULT] " + msg;
    }
}
