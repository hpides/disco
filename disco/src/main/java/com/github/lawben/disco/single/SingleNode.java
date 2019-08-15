package com.github.lawben.disco.single;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.slicing.SlicingWindowOperator;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class SingleNode implements Runnable {

    private final int streamPort;
    private final String resultPath;

    private SlicingWindowOperator<Integer> slicer;
    private long watermarkMs;

    private final static long TIMEOUT_MS = 5 * 1000;

    public SingleNode(int streamPort, String resultPath) {
        this.streamPort = streamPort;
        this.resultPath = resultPath;

        this.slicer = new SlicingWindowOperator<>(new MemoryStateFactory());
        this.slicer.addWindowFunction(DistributedUtils.aggregateFunctionSum());
        this.slicer.addWindowAssigner(new TumblingWindow(WindowMeasure.Time, 1000));
        this.watermarkMs = 1000;
    }

    @Override
    public void run() {
        try (ZContext context = new ZContext()) {
            System.out.println(this.nodeString("Starting single on port " + this.streamPort + " with results to " + this.resultPath));

            ZMQ.Socket streamInput = context.createSocket(SocketType.PULL);
            streamInput.bind(DistributedUtils.buildBindingTcpUrl(this.streamPort));

            ZMQ.Socket resultPusher = context.createSocket(SocketType.PUSH);
            resultPusher.connect(DistributedUtils.buildIpcUrl(this.resultPath));

            ZMQ.Poller streamPoller = context.createPoller(1);
            streamPoller.register(streamInput);

            long currentEventTime = 0;
            long lastWatermark = 0;
            long numEvents = 0;

            while (!Thread.currentThread().isInterrupted()) {
                if (streamPoller.poll(TIMEOUT_MS) == 0) {
                    System.out.println(this.nodeString("Processed " + numEvents + " events in total."));
                    final long watermarkTimestamp = currentEventTime + this.watermarkMs;
                    this.processWatermarkedWindows(resultPusher, watermarkTimestamp);
                    System.out.println(this.nodeString("No more data to come. Ending child worker..."));
                    return;
                }

                String streamId = streamInput.recvStr();
                long eventTimestamp = Long.parseLong(streamInput.recvStr());
                byte[] eventValueBytes = streamInput.recv();
                Object eventValueRaw = DistributedUtils.bytesToObject(eventValueBytes);
                Integer eventValue = (Integer) eventValueRaw;

                this.slicer.processElement(eventValue, eventTimestamp);
                currentEventTime = eventTimestamp;
                numEvents++;

                // If we haven't processed a watermark in watermarkMs milliseconds, process it.
                final long watermarkTimestamp = lastWatermark + this.watermarkMs;
                if (currentEventTime >= watermarkTimestamp) {
                    this.processWatermarkedWindows(resultPusher, watermarkTimestamp);
                    lastWatermark = watermarkTimestamp;
                }
            }
        }
    }

    private void processWatermarkedWindows(Socket resultPusher, long watermarkTimestamp) {
        List<AggregateWindow> aggregateWindows = this.slicer.processWatermark(watermarkTimestamp);

        for (AggregateWindow aggWindow : aggregateWindows) {
            WindowAggregateId windowId = aggWindow.getWindowAggregateId();

            List aggValues = aggWindow.getAggValues();
            Object finalAggregate = aggValues.isEmpty() ? null : aggValues.get(0);
            byte[] finalAggregateBytes = DistributedUtils.objectToBytes(finalAggregate);

            // TODO: fix
            FunctionWindowAggregateId functionWindowAggId = new FunctionWindowAggregateId(windowId, 0);
            resultPusher.sendMore(DistributedUtils.functionWindowIdToString(functionWindowAggId));
            resultPusher.send(finalAggregateBytes);
        }
    }

    private String nodeString(String msg) {
        return "[SINGLE] " + msg;
    }
}
