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

            while (!Thread.currentThread().isInterrupted()) {
                final String rawAggIdOrStreamEnd = resultListener.recvStr();
                if (rawAggIdOrStreamEnd.equals(DistributedUtils.STREAM_END)) {
                    System.out.println(this.resultString("Received stream end. Shutting down..."));
                    return;
                }

                final String rawAggregatedResult = resultListener.recvStr(ZMQ.DONTWAIT);

                final FunctionWindowAggregateId functionWindoAggId =
                        DistributedUtils.stringToFunctionWindowAggId(rawAggIdOrStreamEnd);

                final Integer finalAggregate = Integer.valueOf(rawAggregatedResult);
                System.out.println(this.resultString("FINAL WINDOW: " + functionWindoAggId + " --> " + finalAggregate));
            }
        }
    }

    private String resultString(String msg) {
        return "[RESULT] " + msg;
    }
}
