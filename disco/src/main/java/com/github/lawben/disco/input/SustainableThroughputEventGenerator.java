package com.github.lawben.disco.input;

import com.github.lawben.disco.Event;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Supplier;
import org.zeromq.ZMQ;

/**
 * Keeps queue of events and checks if back pressure is building up.
 * If the back pressure becomes to high, throws an exception.
 */
public class SustainableThroughputEventGenerator {
    private static final int NUM_CHUNKS = 10;
    private static final int QUEUE_BUFFER_FACTOR = 10;
    private static final int MILLIS_IN_SECOND = 1000;

    private final int streamId;
    private final int numEventsPerSecond;
    private final long startTimestamp;
    private final Function<Long, Long> dataSupplier;

    private final Queue<Event> eventQueue;
    private final int queueCapacity;

    private boolean interrupt;

    public SustainableThroughputEventGenerator(int streamId, int numEventsPerSecond, long startTimestamp,
            Function<Long, Long> dataSupplier) {
        this.streamId = streamId;
        this.numEventsPerSecond = numEventsPerSecond;
        this.startTimestamp = startTimestamp;
        this.dataSupplier = dataSupplier;
        this.interrupt = false;
        // Allocate QUEUE_BUFFER_FACTOR times as much space as should be sent per second so we can track the
        // back pressure for QUEUE_BUFFER_FACTOR seconds.
        this.queueCapacity = numEventsPerSecond * QUEUE_BUFFER_FACTOR;
        this.eventQueue = new ArrayBlockingQueue<>(queueCapacity);
    }

    public final void generateNextSecondEvents() {
        final int eventsPerChunk = numEventsPerSecond / NUM_CHUNKS;
        final long generationStart = System.currentTimeMillis();
        final long secondEnd = generationStart + MILLIS_IN_SECOND;

        long totalGenerationTime = 0;
        long totalSleepTime = 0;

        final long currentQueueSize = eventQueue.size();
        if (currentQueueSize + numEventsPerSecond > queueCapacity) {
            throw new IllegalStateException("Queue too full! Cannot insert " + numEventsPerSecond + " events into queue"
                    + " with size " + currentQueueSize + " and capacity " + queueCapacity + ".");
        }

        for (int chunkNum = 1; chunkNum <= NUM_CHUNKS; chunkNum++) {
            final long chunkStart = System.currentTimeMillis();
            for (int eventNum = 0; eventNum < eventsPerChunk; eventNum++) {
                final long eventTimestamp = System.currentTimeMillis() - startTimestamp;
                final long eventValue = dataSupplier.apply(eventTimestamp);
                eventQueue.add(new Event(eventValue, eventTimestamp, this.streamId));
            }
            final long remainingChunks = NUM_CHUNKS - chunkNum;

            final long chunkEnd = System.currentTimeMillis();
            final long chunkDuration = chunkEnd - chunkStart;
            totalGenerationTime += chunkDuration;

            if (chunkNum == NUM_CHUNKS) {
                // We are at end of the generation second. No need to sleep.
                break;
            }

            final long remainingInSecond = secondEnd - chunkEnd;
            final long estimatedDurationForRemainingChunks = remainingChunks * (totalGenerationTime / chunkNum);
            // At least 0 so we don't get negative sleep times.
            final long estimatedNeededSleep = Math.max(0, remainingInSecond - estimatedDurationForRemainingChunks);
            final long sleepForCurrentChunk = estimatedNeededSleep / remainingChunks;

            totalSleepTime += sleepForCurrentChunk;
            try {
                Thread.sleep(sleepForCurrentChunk);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Sleep in event generation errored. ERROR: " + e);
            }
        }
        final long generationEnd = System.currentTimeMillis();
        final long realGenerationDuration = generationEnd - generationStart;
        final long generationDifference = generationEnd - secondEnd;
        System.out.println("Generated " + numEventsPerSecond +
                " in " + realGenerationDuration + "ms" +
                " (" + generationDifference + "ms deviation)." +
                " Total generation time: " + totalGenerationTime + "ms." +
                " Total sleep time: " + totalSleepTime + "ms."
        );

    }

    public Queue<Event> getEventQueue() {
        return eventQueue;
    }

    public void interrupt() {
        System.out.println("Generator was interrupted and will end soon.");
        interrupt = true;
    }

    public boolean isInterrupted() {
        return interrupt;
    }
}

