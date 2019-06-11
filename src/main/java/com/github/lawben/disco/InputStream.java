package com.github.lawben.disco;

import java.util.Random;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class InputStream<T> implements Runnable {

    protected final int streamId;
    private final InputStreamConfig<T> config;
    private final String nodeIp;
    private final int nodePort;
    private EventGenerator<T> eventGenerator;

    public InputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort, EventGenerator<T> eventGenerator) {
        this.streamId = streamId;
        this.config = config;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
        this.eventGenerator = eventGenerator;
    }

    protected String streamName() { return this.eventGenerator.getClass().getSimpleName(); }

    @Override
    public void run() {
        System.out.println(this.streamIdString("Starting " + this.streamName() + " with " + this.config.numEventsToSend
                + " events to node " + this.nodeIp + ":" + this.nodePort + " with " + this.config));

        System.out.println(this.streamIdString("Using seed: " + this.config.randomSeed));
        Random rand = new Random(this.config.randomSeed);

        try (ZContext context = new ZContext()) {
            this.registerAtNode(context);

            Thread.sleep(DistributedChild.STREAM_REGISTER_TIMEOUT_MS * 2);

            ZMQ.Socket eventSender = context.createSocket(SocketType.PUSH);
            eventSender.setHWM(this.config.numEventsToSend);
            eventSender.connect(DistributedUtils.buildTcpUrl(this.nodeIp, this.nodePort));

            System.out.println(this.streamIdString("Start sending data"));

            final long sendingStartTime = System.currentTimeMillis();
            long lastEventTimestamp = this.eventGenerator.generateAndSendEvents(rand, eventSender);
            final long duration = System.currentTimeMillis() - sendingStartTime;
            eventSender.sendMore(DistributedUtils.STREAM_END);
            eventSender.send(String.valueOf(this.streamId));
            System.out.println(this.streamIdString("Last event timestamp: " + lastEventTimestamp +
                    " (total sending duration: " + duration + "ms)"));

            // Allow stream end to be processed before killing the context
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println(this.streamIdString("Finished sending events. Shutting down..."));
    }


    protected void registerAtNode(ZContext context) {
        System.out.println(this.streamIdString("Registering at node."));
        final ZMQ.Socket nodeRegistrar = context.createSocket(SocketType.REQ);
        nodeRegistrar.connect(DistributedUtils.buildTcpUrl(this.nodeIp, this.nodePort + DistributedChild.STREAM_REGISTER_PORT_OFFSET));

        nodeRegistrar.send(String.valueOf(this.streamId));
        nodeRegistrar.recv();
        System.out.println(this.streamIdString("Registering successful."));
    }

    private String streamIdString(String msg) {
        return "[STREAM-" + this.streamId + "] " + msg;
    }


}
