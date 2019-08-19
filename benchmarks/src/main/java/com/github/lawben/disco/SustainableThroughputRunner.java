package com.github.lawben.disco;

import static com.github.lawben.disco.DistributedChild.STREAM_REGISTER_PORT_OFFSET;
import static com.github.lawben.disco.DistributedUtils.STREAM_END;

import com.github.lawben.disco.input.SustainableThroughputEventGenerator;
import java.util.Queue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;


public class SustainableThroughputRunner {
    private static final int SEND_CHUNK_SIZE = 1000;
    private static final long SEND_PERIOD_DURATION_MS = 500;
    private static final long MAX_INCREASE_STREAK = 10;

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Required args: streamId eventsPerSecond totalRunTimeInSeconds nodeAddress\n" +
                    "e.g. java SustainableThroughputRunner 0 100000 60 127.0.0.1:4060\n" +
                    "This will generate 100.000 events per second for 60 seconds (6 mio. events in total) "
                    + "and send them to localhost on port 4060 from stream with id 0.");
            System.exit(1);
        }

        final int streamId = Integer.parseInt(args[0]);
        final int eventsPerSec = Integer.parseInt(args[1]);
        final long totalDuration = Long.parseLong(args[2]);
        final String nodeAddress = args[3];

        System.out.println("Running sustainable throughput generator for " + totalDuration + " seconds with " +
                eventsPerSec + " events/s to " + nodeAddress);

        ZContext context = new ZContext();
        ZMQ.Socket dataPusher = context.createSocket(SocketType.PUSH);
        dataPusher.setSndHWM(100);
        dataPusher.connect("tcp://" + nodeAddress);

        // Register at parent and wait for it to set up correctly.
        final String nodeIP = nodeAddress.split(":")[0];
        final int dataPort = Integer.parseInt(nodeAddress.split(":")[1]);
        final ZMQ.Socket nodeRegistrar = context.createSocket(SocketType.REQ);
        nodeRegistrar.connect(DistributedUtils.buildTcpUrl(nodeIP, dataPort + STREAM_REGISTER_PORT_OFFSET));
        nodeRegistrar.send(String.valueOf(streamId));
        nodeRegistrar.recv();
        Thread.sleep(1000);

        // Time
        final long startTime = System.currentTimeMillis();
        final long totalDurationInMillis = 1000 * totalDuration;
        final long endTime = startTime + totalDurationInMillis;

        // Generator
        final Function<Long, Long> onesGenerator = (eventTimestamp) -> 1L;
        final Function<Long, Long> timestampGenerator = (eventTimestamp) -> eventTimestamp;
        final SustainableThroughputEventGenerator generator =
                new SustainableThroughputEventGenerator(0, eventsPerSec, startTime, onesGenerator);

        GeneratorException generatorException = new GeneratorException();
        Thread.UncaughtExceptionHandler generatorThreadExceptionHandler =
                (thread, exception) -> generatorException.setException(exception);

        Thread generatorThread = new Thread(() -> {
            while (!generator.isInterrupted() && System.currentTimeMillis() < startTime + totalDuration * 1000) {
                generator.generateNextSecondEvents();
            }
            System.out.println("ENDING GENERATOR");
        });
        generatorThread.setUncaughtExceptionHandler(generatorThreadExceptionHandler);
        generatorThread.start();

        // Wait for event generator to start
        Thread.sleep(1000);

        // Event sending
        Queue<Event> eventQueue = generator.getEventQueue();
        long queueSize = eventQueue.size();
        int currentIncreaseStreak = 0;

        while (System.currentTimeMillis() < endTime) {
            long nextSendPeriodEnd = System.currentTimeMillis() + SEND_PERIOD_DURATION_MS;
            LongAdder sentCounter = new LongAdder();
            // Send data for SEND_PERIOD_DURATION_MS
            while (System.currentTimeMillis() < nextSendPeriodEnd) {
                if (!sendDataChunk(dataPusher, eventQueue, sentCounter)) {
                    // Generation too slow, skip period and check duration.ß
                    break;
                }
            }

            if (generatorException.wasThrown()) {
                endStream(streamId, dataPusher);
                throw new RuntimeException("Generator exception was thrown!\n" + generatorException.getException());
            }

            System.out.println("Sent " + sentCounter + " events in " + SEND_PERIOD_DURATION_MS + "ms.");

            // Check queue size after sending for a while.
            final long currentQueueSize = eventQueue.size();
            final long increaseSinceLastChunk = currentQueueSize - queueSize;
            queueSize = currentQueueSize;
            System.out.println("Current queue size: " + queueSize);
            if (queueSize > (5 * eventsPerSec) && increaseSinceLastChunk > 0) {
                // Queue is growing in size
                if (++currentIncreaseStreak == MAX_INCREASE_STREAK) {
                    generator.interrupt();
                    endStream(streamId, dataPusher);
                    throw new IllegalStateException("Unsustainable throughput! Queue size is " + queueSize +
                            " and has increased " + MAX_INCREASE_STREAK + " times in a row.");
                }
            } else {
                currentIncreaseStreak = 0;
            }
        }

        endStream(streamId, dataPusher);
        generatorThread.join();
    }

    public static boolean sendDataChunk(Socket dataPusher, Queue<Event> eventQueue, LongAdder sentCounter) throws InterruptedException {
        int numSleepsInSendPeriod = 0;

        // Send SEND_CHUNK_SIZE to avoid system call every iteration.
        for (int i = 0; i < SEND_CHUNK_SIZE; i++) {
            Event event = eventQueue.poll();
            if (event == null) {
                // Sleep for a while to avoid blocking the data generator too much while pushing data.
                numSleepsInSendPeriod++;
                Thread.sleep(5);
                if (numSleepsInSendPeriod == 10) {
                    // Data generation is slow. Slept 10 times in last send period.
                    return false;
                }
                continue;
            }

            dataPusher.send(event.asString());
            sentCounter.increment();
        }

        return true;
    }

    private static void endStream(int streamId, Socket dataPusher) {
        dataPusher.sendMore(STREAM_END);
        dataPusher.send(String.valueOf(streamId));
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
