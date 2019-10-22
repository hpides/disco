package com.github.lawben.disco.executables;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.HIGH_WATERMARK;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.SustainableThroughputEventGenerator;
import com.github.lawben.disco.SustainableThroughputGenerator;
import com.github.lawben.disco.SustainableThroughputWindowGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;


public class SustainableThroughputRunner {
    private static int NUM_SENDERS = 4;
    static final long SEND_PERIOD_DURATION_MS = 1000;
    private static final long MAX_INCREASE_STREAK = 10;
    private static final long WARM_UP_PART = 4;
    static int SEND_CHUNK_SIZE = 10000;

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Required args: streamId nodeAddress eventsPerSecond totalRunTimeInSeconds aggFn childX|stream\n" +
                    "e.g. java SustainableThroughputRunner 0 127.0.0.1:4060 100000 60\n" +
                    "This will generate 100.000 events per second for 60 seconds (6 mio. events in total) "
                    + "and send them to localhost on port 4060 from stream with id 0.");
            System.exit(1);
        }

        final int streamId = Integer.parseInt(args[0]);
        final String nodeAddress = args[1];
        final int eventsPerSec = Integer.parseInt(args[2]);
        final long totalDuration = Long.parseLong(args[3]);
        final String aggFn = args[4];
        final String mode = args[5];
        assert mode.startsWith("child") || mode.equals("stream");

        final boolean isStream = mode.equals("stream");
        int numChildren = 0;
        if (!isStream) {
            numChildren = Integer.parseInt(mode.replace("child", ""));
            SEND_CHUNK_SIZE = 1000;
            NUM_SENDERS = numChildren;
            if (aggFn.equals("M_MEDIAN")) {
                SEND_CHUNK_SIZE = 100;
            }
        } else {
            if (aggFn.equals("M_MEDIAN")) {
                SEND_CHUNK_SIZE = 1000;
            }
        }

        System.out.println("Running sustainable " + mode + " throughput generator for " + totalDuration +
                " seconds with " + eventsPerSec + " events/s to " + nodeAddress);

        ZContext context = new ZContext();
        List<Socket> senders = new ArrayList<>(NUM_SENDERS);
        for (int socketNum = 0; socketNum < NUM_SENDERS; socketNum++) {
            Socket dataPusher = context.createSocket(SocketType.PUSH);
            dataPusher.setSndHWM(HIGH_WATERMARK);
            dataPusher.connect("tcp://" + nodeAddress);
            senders.add(dataPusher);
        }

        Socket streamEndSender = context.createSocket(SocketType.PUSH);
        streamEndSender.connect("tcp://" + nodeAddress);

        // Register at parent and wait for it to set up correctly.
        final String nodeIP = nodeAddress.split(":")[0];
        final int dataPort = Integer.parseInt(nodeAddress.split(":")[1]);

        final long startTime;
        if (isStream) {
            final Socket nodeRegistrar = context.createSocket(SocketType.REQ);
            nodeRegistrar.setReceiveTimeOut(30 * 1000);
            nodeRegistrar.connect(DistributedUtils.buildTcpUrl(nodeIP, dataPort + STREAM_REGISTER_PORT_OFFSET));
            nodeRegistrar.send(String.valueOf(streamId));
            String startTimeString = nodeRegistrar.recvStr();
            if (startTimeString == null) {
                throw new RuntimeException("Could not register at child node.");
            }
            startTime = Long.parseLong(startTimeString);
        } else {
            int registerPortOffset = -1;
            long partialStartTime = 0;
            for (int i = 0; i < numChildren; i++) {
                final Socket nodeRegistrar = context.createSocket(SocketType.REQ);
                nodeRegistrar.setReceiveTimeOut(15 * 1000);
                nodeRegistrar.connect(DistributedUtils.buildTcpUrl(nodeIP, dataPort + registerPortOffset));
                nodeRegistrar.send("[NODE-" + i + "] I am a new node");
                nodeRegistrar.recvStr();
                partialStartTime = Long.parseLong(nodeRegistrar.recvStr());
                nodeRegistrar.recvStr();
                nodeRegistrar.recvStr();
                System.out.println("Registered generator " + i);
            }
            startTime = partialStartTime;
        }

        Thread.sleep(1000);

        // Time
        final long totalDurationInMillis = 1000 * totalDuration;
        final long warmUpTime = totalDurationInMillis / WARM_UP_PART;
        final long endTime = startTime + totalDurationInMillis;
        final long warmUpEndTime = startTime + warmUpTime;

        List<GeneratorSetup> generators = new ArrayList<>(NUM_SENDERS);
        for (int i = 0; i < NUM_SENDERS; i++) {
            final int eventsPerGenerator = eventsPerSec / NUM_SENDERS;
            generators.add(startGenerator(eventsPerGenerator, startTime, aggFn, totalDurationInMillis,
                    numChildren, isStream));
        }

        List<Thread> senderThreads = new ArrayList<>(NUM_SENDERS);
        List<DataSender> dataSenders = new ArrayList<>(NUM_SENDERS);
        for (int senderId = 0; senderId < NUM_SENDERS; senderId++) {
            Socket dataPusher = senders.get(senderId);
            GeneratorSetup generatorSetup = generators.get(senderId);
            DataSender dataSender = new DataSender(dataPusher, generatorSetup, senderId);
            Thread senderThread = new Thread(() -> dataSender.sendDataUntil(endTime), "sender-" + senderId);
            senderThread.start();
            senderThreads.add(senderThread);
            dataSenders.add(dataSender);
        }

        // Event sending
        List<ArrayBlockingQueue<List<String>>> eventQueues = generators.stream()
                .map(g -> g.generator.getEventQueue())
                .collect(Collectors.toList());

        int queueSize = getTotalQueueSize(eventQueues);
        int currentIncreaseStreak = 0;
        boolean warmedUp = false;

        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(SEND_PERIOD_DURATION_MS);

            if (!warmedUp && System.currentTimeMillis() > warmUpEndTime) {
                System.out.println("Clearing event queue after warm up time");
                warmedUp = true;
                final long emptyStart = System.currentTimeMillis();
                eventQueues.forEach(Queue::clear);
                System.out.println("Clearing took " + (System.currentTimeMillis() - emptyStart) + " ms.");
                queueSize = 0;
            }

            for (GeneratorSetup generatorSetup : generators) {
                GeneratorException generatorException = generatorSetup.exception;
                if (generatorException.wasThrown()) {
                    System.out.println("Ending stream because of generator exception.");
                    endRunner(streamId, context, senders, streamEndSender, generators, senderThreads, dataSenders, numChildren);
                    throw new RuntimeException("Generator exception was thrown!\n" + generatorException.getException());
                }
            }

            // Check queue size after sending for a while.
            final int currentQueueSize = getTotalQueueSize(eventQueues);
            final int increaseSinceLastChunk = currentQueueSize - queueSize;
            queueSize = currentQueueSize;
            System.out.println("Current queue size: " + queueSize);
            if (warmedUp && queueSize > (2 * eventsPerSec) && increaseSinceLastChunk > 0) {
                // Queue is growing in size
                if (++currentIncreaseStreak == MAX_INCREASE_STREAK) {
                    System.out.println("Ending stream because of unsustainable throughput.");
                    endRunner(streamId, context, senders, streamEndSender, generators, senderThreads, dataSenders, numChildren);
                    throw new IllegalStateException("Unsustainable throughput! Queue size is " + queueSize +
                            " and has increased " + MAX_INCREASE_STREAK + " times in a row.");
                }
            } else {
                currentIncreaseStreak = 0;
            }
        }

        System.out.println("Ending stream after " + totalDuration + " seconds.");
        endRunner(streamId, context, senders, streamEndSender, generators, senderThreads, dataSenders, numChildren);
    }

    private static int getTotalQueueSize(List<ArrayBlockingQueue<List<String>>> eventQueues) {
        return eventQueues.stream().map(Queue::size).reduce(Integer::sum).orElseThrow();
    }

    public static GeneratorSetup startGenerator(int eventsPerSec, long startTime, String aggFn, long totalDurationInMillis,
            int nodeId, boolean isStream) {
        final SustainableThroughputGenerator generator = isStream
                ? new SustainableThroughputEventGenerator(nodeId, eventsPerSec, startTime)
                : new SustainableThroughputWindowGenerator(nodeId, eventsPerSec, aggFn);

        GeneratorException generatorException = new GeneratorException();
        Thread.UncaughtExceptionHandler generatorThreadExceptionHandler =
                (thread, exception) -> generatorException.setException(exception);

        Thread generatorThread = new Thread(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!generator.isInterrupted() && System.currentTimeMillis() < startTime + totalDurationInMillis) {
                generator.generateNextSecondEvents();
            }
            System.out.println("ENDING GENERATOR");
        });
        generatorThread.setUncaughtExceptionHandler(generatorThreadExceptionHandler);
        generatorThread.start();

        return new GeneratorSetup(generator, generatorException, generatorThread);
    }

    private static void endRunner(int streamId, ZContext context, List<Socket> senders, Socket streamEndSender,
            List<GeneratorSetup> generators, List<Thread> senderThreads, List<DataSender> dataSenders, int numChildren)
            throws InterruptedException {
        generators.forEach(g -> g.generator.interrupt());
        dataSenders.forEach(DataSender::interrupt);

        System.out.println("Ending runner...");
        for (GeneratorSetup generatorSetup : generators) {
            generatorSetup.thread.join();
        }
        for (Thread senderThread : senderThreads) {
            senderThread.join();
        }
        Thread.sleep(SEND_PERIOD_DURATION_MS);

        System.out.println("Ending stream...");
        if (streamId != -1) {
            streamEndSender.sendMore(STREAM_END);
            streamEndSender.send(String.valueOf(streamId));
        } else {
            for (int i = 0; i < numChildren; i++) {
                streamEndSender.sendMore(STREAM_END);
                streamEndSender.send(String.valueOf(i));
            }
        }

        // Wait for child to receive stream end before killing connection.
        Thread.sleep(5000);
        senders.forEach(Socket::close);
        streamEndSender.close();
        context.close();
    }
}

class DataSender {

    private final Socket dataPusher;
    private final GeneratorSetup generatorSetup;
    private final int id;

    private boolean interrupt;

    public DataSender(Socket dataPusher, GeneratorSetup generatorSetup, int id) {
        this.dataPusher = dataPusher;
        this.generatorSetup = generatorSetup;
        this.id = id;
    }

    public void sendDataUntil(final long endTime) {
        while (!interrupt && System.currentTimeMillis() < endTime) {
            final long sendStart = System.currentTimeMillis();
            final long nextSendPeriodEnd = sendStart + SustainableThroughputRunner.SEND_PERIOD_DURATION_MS;
            LongAdder sentCounter = new LongAdder();

            while (!interrupt && System.currentTimeMillis() < nextSendPeriodEnd) {
                sendDataChunk(dataPusher, generatorSetup.generator.getEventQueue(), sentCounter);
            }

            final long sendEnd = System.currentTimeMillis();
            System.out.println("[" + id + "] Sent " + sentCounter + " events in " +
                    (sendEnd - sendStart) + "ms.");
        }

        if (interrupt) {
            System.out.println("Ending sender-" + id + " because of interrupt.");
        }
    }

    private void sendDataChunk(Socket dataPusher, Queue<List<String>> eventQueue, LongAdder sentCounter) {
        int numSleepsInSendPeriod = 0;

        // Send SEND_CHUNK_SIZE to avoid system call every iteration.
        for (int i = 0; i < SustainableThroughputRunner.SEND_CHUNK_SIZE; i++) {
            List<String> msg = eventQueue.poll();
            if (msg == null) {
                // Sleep for a while to avoid blocking the data generator too much while pushing data.
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (++numSleepsInSendPeriod == 5) {
                    return;
                }

                continue;
            }

            for (int msgPart = 0; msgPart < msg.size() - 1; msgPart++) {
                dataPusher.sendMore(msg.get(msgPart));
            }
            this.dataPusher.send(msg.get(msg.size() - 1));

            sentCounter.increment();
        }
    }

    public void interrupt() {
        this.interrupt = true;
    }
}

class GeneratorException {
    private boolean wasThrown;
    private Throwable exception;

    public GeneratorException() {
        this(false, null);
    }

    public GeneratorException(boolean wasThrown, Throwable exception) {
        this.wasThrown = wasThrown;
        this.exception = exception;
    }

    public boolean wasThrown() {
        return wasThrown;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.wasThrown = true;
        this.exception = exception;
    }
}

class GeneratorSetup {
    SustainableThroughputGenerator generator;
    GeneratorException exception;
    Thread thread;

    public GeneratorSetup(SustainableThroughputGenerator generator,
            GeneratorException exception, Thread thread) {
        this.generator = generator;
        this.exception = exception;
        this.thread = thread;
    }
}
