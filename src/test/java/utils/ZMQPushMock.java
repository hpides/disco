package utils;

import com.github.lawben.disco.DistributedUtils;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.SocketType;

public class ZMQPushMock extends ZMQMock {
    public ZMQPushMock(int port) {
        this(new ArrayList<>(), port);
    }

    public ZMQPushMock(List<List<String>> messages, int port) {
        super(messages);
        this.socket =  context.createSocket(SocketType.PUSH);
        this.socket.connect(DistributedUtils.buildTcpUrl("localhost", port));
    }

    public void sendNext() {
        if (msgCounter < messages.size()) {
            List<String> msgParts = messages.get(msgCounter);
            for (int i = 0; i < msgParts.size() - 1; i++) {
                this.socket.sendMore(msgParts.get(i));
            }
            this.socket.send(msgParts.get(msgParts.size() - 1));

            msgCounter++;
            return;
        }

        throw new IllegalStateException("No more messages to send");
    }
}
