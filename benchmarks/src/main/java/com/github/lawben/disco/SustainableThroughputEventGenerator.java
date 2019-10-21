package com.github.lawben.disco;

import java.util.List;
import java.util.function.Function;

/**
 * Keeps queue of events and checks if back pressure is building up.
 * If the back pressure becomes to high, throws an exception.
 */
public class SustainableThroughputEventGenerator extends SustainableThroughputGenerator {

    public static Function<Long, List<String>> getEventGen(final long startTime, final int streamId) {
        return (realTime) -> {
            final long eventTimestamp = realTime - startTime;
            final long eventValue = realTime;
            return List.of(new Event(eventValue, eventTimestamp, streamId).asString());
        };
    }

    public SustainableThroughputEventGenerator(int streamId, int numEventsPerSecond, long startTimestamp) {
        super(numEventsPerSecond, getEventGen(startTimestamp, streamId));
    }
}

