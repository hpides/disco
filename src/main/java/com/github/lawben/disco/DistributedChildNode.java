package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedUtils.createAggFunctionsFromString;
import static com.github.lawben.disco.DistributedUtils.createWindowsFromString;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import java.util.List;
import java.util.stream.Collectors;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

class DistributedChildNode extends DistributedNode {
    final String parentIp;
    final int parentControllerPort;
    final int parentWindowPort;

    protected long watermarkMs;

    public DistributedChildNode(int childId, String nodeIdentifier, String parentIp, int parentControllerPort, int parentWindowPort) {
        super(childId, nodeIdentifier);
        this.parentIp = parentIp;
        this.parentControllerPort = parentControllerPort;
        this.parentWindowPort = parentWindowPort;
    }

    public WindowingConfig registerAtParent() {
        ZMQ.Socket controlClient = this.context.createSocket(SocketType.REQ);
        controlClient.connect(DistributedUtils.buildTcpUrl(this.parentIp, this.parentControllerPort));

        controlClient.send(this.nodeString("I am a new node."));

        this.watermarkMs = Long.parseLong(controlClient.recvStr());
        String windowString = controlClient.recvStr();
        String aggString = controlClient.recvStr();
        controlClient.close();
        System.out.println(this.nodeString("Received: " + this.watermarkMs +
                " ms watermark | [" + windowString.replace("\n", ";") + "] | [" + aggString.replace("\n", ";") + "]"));

        this.windowPusher = this.context.createSocket(SocketType.PUSH);
        this.windowPusher.connect(DistributedUtils.buildTcpUrl(this.parentIp, this.parentWindowPort));

        List<Window> allWindows = createWindowsFromString(windowString);
        List<AggregateFunction> aggregateFunctions = createAggFunctionsFromString(aggString);

        List<Window> timeWindows = allWindows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Time)
                .collect(Collectors.toList());

        List<Window> countWindows = allWindows.stream()
                .filter(w -> w.getWindowMeasure() == WindowMeasure.Count)
                .collect(Collectors.toList());

        return new WindowingConfig(timeWindows, countWindows, aggregateFunctions);
    }
}
