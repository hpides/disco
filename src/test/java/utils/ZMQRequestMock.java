package utils;

import com.github.lawben.disco.DistributedUtils;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.SocketType;

public class ZMQRequestMock extends ZMQMock {
    public ZMQRequestMock(int port) {
        this(new ArrayList<>(), port);
    }

    public ZMQRequestMock(List<List<String>> messages, int port) {
        super(messages);
        this.socket =  context.createSocket(SocketType.REQ);
        this.socket.connect(DistributedUtils.buildTcpUrl("localhost", port));
    }

    public List<String> requestNext() {
        return requestNext(1);
    }

    public List<String> requestNext(int numExpectedMessageParts) {
        if (msgCounter < messages.size()) {
            List<String> msgParts = messages.get(msgCounter);
            if (msgParts.isEmpty()) {
                throw new IllegalStateException("ZMQRequestMock needs at least one message part per request.");
            }

            for (int i = 0; i < msgParts.size() - 1; i++) {
                this.socket.sendMore(msgParts.get(i));
            }
            this.socket.send(msgParts.get(msgParts.size() - 1));

            List<String> result = new ArrayList<>(numExpectedMessageParts);
            for (int i = 0; i < numExpectedMessageParts; i++) {
                result.add(this.socket.recvStr());
            }

            msgCounter++;
            return result;
        }

        throw new IllegalStateException("No more requests to send");
    }
}
