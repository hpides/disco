package utils;

import java.util.Arrays;
import java.util.List;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public abstract class ZMQMock implements AutoCloseable {
    protected final List<List<String>> messages;
    protected final ZContext context;
    protected int msgCounter;
    protected ZMQ.Socket socket;

    final static int RECEIVE_TIMEOUT_MS = 3000;

    public ZMQMock(List<List<String>> messages) {
        this.messages = messages;
        this.context = new ZContext();
        this.msgCounter = 0;
    }

    public void addMessage(List<String> message) {
        this.messages.add(message);
    }

    public void addMessage(String... messageParts) {
        addMessage(Arrays.asList(messageParts));
    }

    @Override
    public void close() {
        if (socket != null) {
            this.socket.close();
        }
        this.context.close();
    }
}
