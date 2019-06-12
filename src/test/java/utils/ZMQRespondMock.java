package utils;

import com.github.lawben.disco.DistributedUtils;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ZMQRespondMock extends ZMQMock {
    public ZMQRespondMock(int port) {
        this(new ArrayList<>(), port);
    }

    public ZMQRespondMock(List<List<String>> messages, int port) {
        super(new ArrayList<>());
        this.socket =  context.createSocket(SocketType.REP);
        this.socket.bind(DistributedUtils.buildBindingTcpUrl(port));
    }

    public List<String> respondToNext() {
        return respondToNext(1);
    }

    public List<String> respondToNext(int numMessageParts) {
        if (msgCounter < messages.size()) {
            List<String> result = new ArrayList<>(numMessageParts);
            for (int i = 0; i < numMessageParts; i++) {
                result.add(this.socket.recvStr());
            }

            List<String> msgParts = messages.get(msgCounter);
            if (msgParts.isEmpty()) {
                throw new IllegalStateException("ZMQRespondMock needs at least one message part per response.");
            }

            for (int i = 0; i < msgParts.size() - 1; i++) {
                this.socket.sendMore(msgParts.get(i));
            }
            this.socket.send(msgParts.get(msgParts.size() - 1));

            msgCounter++;
            return result;
        }

        throw new IllegalStateException("No more responses to send");
    }
}
