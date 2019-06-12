package utils;

import com.github.lawben.disco.DistributedUtils;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ZMQPullMock extends ZMQMock {
    public ZMQPullMock(int port) {
        super(null);
        this.socket = context.createSocket(SocketType.PULL);
        this.socket.bind(DistributedUtils.buildBindingTcpUrl(port));
    }

    public List<String> receiveNext() {
        return receiveNext(1);
    }

    public List<String> receiveNext(int numMessageParts) {
        List<String> result = new ArrayList<>(numMessageParts);
        for (int i = 0; i < numMessageParts; i++) {
            result.add(this.socket.recvStr());
        }
        return result;
    }
}
