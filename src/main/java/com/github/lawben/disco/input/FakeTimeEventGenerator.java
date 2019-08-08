package com.github.lawben.disco.input;

import java.util.Random;
import org.zeromq.ZMQ;

/**
 * Uses the random function to advance the timestamp of the events but not actual time. This is deterministic in the
 * creation of the events given rand with the same seed. Network-related code is not deterministic.
 */
public class FakeTimeEventGenerator<T> implements EventGenerator<T> {
    private final int streamId;
    private final InputStreamConfig<T> config;

    public FakeTimeEventGenerator(int streamId, InputStreamConfig<T> config) {
        this.streamId = streamId;
        this.config = config;
    }

    @Override
    public long generateAndSendEvents(Random rand, ZMQ.Socket eventSender) {
        int numEvents = config.numEventsToSend;
        int[] eventValues = new int[numEvents];
        long[] eventTimestamps = new long[numEvents];

        long lastEventTimestamp = 0;

        int max = config.maxWaitTimeMillis;
        int min = config.minWaitTimeMillis;

        // Generate all events
        System.out.println("[FAKE-GEN] Generating...");
        for (int eventNum = 0; eventNum < numEvents; eventNum++) {
            final int fakeSleepTime = rand.nextInt((max - min) + 1) + min;
            final long eventTimestamp = lastEventTimestamp + fakeSleepTime;
            final Integer eventValue = (Integer) config.generatorFunction.apply(rand);
            eventValues[eventNum] = eventValue;
            eventTimestamps[eventNum] = eventTimestamp;
            lastEventTimestamp = eventTimestamp;
        }

        // Send all events
        System.out.println("[FAKE-GEN] Sending...");
        final String streamIdString = String.valueOf(this.streamId);
        for (int eventNum = 0; eventNum < numEvents; eventNum++) {
            final String msg = streamIdString + ',' + eventTimestamps[eventNum] + ',' + eventValues[eventNum];
            eventSender.send(msg, ZMQ.DONTWAIT);
        }

        return eventTimestamps[numEvents - 1];
    }
}

