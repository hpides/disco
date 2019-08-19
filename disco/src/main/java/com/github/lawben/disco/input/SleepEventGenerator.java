package com.github.lawben.disco.input;

import com.github.lawben.disco.Event;
import java.util.Random;
import java.util.function.Function;
import org.zeromq.ZMQ;

/**
 * Uses the random function to sleep. This causes the event time to progress "normally". The sleep times are
 * deterministic under the same `rand` condition but the sleep is not.
 */
public class SleepEventGenerator implements EventGenerator {
    private final int streamId;
    private final InputStreamConfig config;

    public SleepEventGenerator(int streamId, InputStreamConfig config) {
        this.streamId = streamId;
        this.config = config;
    }

    @Override
    public final long generateAndSendEvents(Random rand, ZMQ.Socket eventSender) throws Exception {
        long lastEventTimestamp = 0;
        final long startTime = config.startTimestamp;
        final Function<Random, Long> eventGenerator = config.generatorFunction;
        final int minSleepTime = config.minWaitTimeMillis;
        final int maxSleepTime = config.maxWaitTimeMillis;

        for (int i = 0; i < config.numEventsToSend; i++) {
            this.doSleep(minSleepTime, maxSleepTime, rand);

            final long eventTimestamp = System.currentTimeMillis() - startTime;
            final Long eventValue = (Long) eventGenerator.apply(rand);
            final Event event = new Event(eventValue, eventTimestamp, this.streamId);
            final String msg = event.asString();
            eventSender.send(msg);

            lastEventTimestamp = eventTimestamp;
        }

        return lastEventTimestamp;
    }

    protected void doSleep(int minSleep, int maxSleep, Random rand) throws InterruptedException {
        int sleepTime = rand.nextInt((maxSleep - minSleep) + 1) + minSleep;
        Thread.sleep(sleepTime);
    }
}

