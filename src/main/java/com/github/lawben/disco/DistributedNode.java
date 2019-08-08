package com.github.lawben.disco;

import java.io.Closeable;
import java.io.IOException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public abstract class DistributedNode {
    private boolean interrupt;
    private final String nodeIdentifier;
    protected final int nodeId;

    protected final ZContext context;
    protected ZMQ.Socket dataPuller;
    protected ZMQ.Socket windowPusher;

    public DistributedNode(int nodeId, String nodeIdentifier) {
        this.nodeId = nodeId;
        this.nodeIdentifier = nodeIdentifier;
        this.context = new ZContext();
        this.interrupt = false;
    }

    public ZMQ.Socket createDataPuller(int port) {
        this.dataPuller = this.context.createSocket(SocketType.PULL);
        this.dataPuller.setReceiveTimeOut(DistributedUtils.DEFAULT_SOCKET_TIMEOUT_MS);
        this.dataPuller.bind(DistributedUtils.buildBindingTcpUrl(port));
        return this.dataPuller;
    }

    public ZMQ.Socket createWindowPusher(String ip, int port) {
        this.windowPusher = this.context.createSocket(SocketType.PUSH);
        this.windowPusher.connect(DistributedUtils.buildTcpUrl(ip, port));
        return this.windowPusher;
    }

    public ZMQ.Socket createWindowPusher(String path) {
        this.windowPusher = this.context.createSocket(SocketType.PUSH);
        this.windowPusher.connect(DistributedUtils.buildIpcUrl(path));
        return this.windowPusher;
    }

    void interrupt() {
        this.interrupt = true;
    }

    boolean isInterrupted() {
        return this.interrupt;
    }

    public String nodeString(String msg) {
        return "[" + this.nodeIdentifier + "-" + this.nodeId + "] " + msg;
    }

    public void endChild() {
        this.windowPusher.sendMore(DistributedUtils.STREAM_END);
        this.windowPusher.send(String.valueOf(this.nodeId));
    }

    public void close() {
        closeIfNotNull(windowPusher);
        closeIfNotNull(dataPuller);
        closeIfNotNull(context);
    }

    private void closeIfNotNull(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {}
        }
    }
}
