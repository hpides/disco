package com.github.lawben.disco.utils;

import com.github.lawben.disco.DistributedUtils;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZMQ.Error;

public class ZMQPullMock extends ZMQMock {
    public ZMQPullMock(int port) {
        super(null);
        this.socket = context.createSocket(SocketType.PULL);
        this.socket.setReceiveTimeOut(ZMQMock.RECEIVE_TIMEOUT_MS);
        this.socket.bind(DistributedUtils.buildBindingTcpUrl(port));
    }

    public List<String> receiveNext() {
        return receiveNext(1);
    }

    public List<String> receiveNext(int numMessageParts) {
        List<String> result = new ArrayList<>(numMessageParts);
        for (int i = 0; i < numMessageParts; i++) {
            String receivedString = this.socket.recvStr();
            if (receivedString == null && this.socket.errno() == Error.EINTR.getCode()) {
                // Timed out
                System.out.println("Receiving timed out");
                break;
            }
            result.add(receivedString);
        }
        return result;
    }
}
