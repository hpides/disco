package com.github.lawben.disco;

import java.util.List;
import java.util.function.Function;

/**
 * Keeps queue of events and checks if back pressure is building up.
 * If the back pressure becomes to high, throws an exception.
 */
public class SustainableThroughputEventGenerator extends SustainableThroughputGenerator {

    public static Function<Long, List<String>> getEventGen(final long startTime, final int streamId, int numKeys) {
        EventGenerator eventGenerator = new EventGenerator(streamId, numKeys, startTime);
        return eventGenerator::getNextWindow;
    }

    public SustainableThroughputEventGenerator(int streamId, int numEventsPerSecond, long startTimestamp, int numKeys) {
        super(numEventsPerSecond, getEventGen(startTimestamp, streamId, numKeys));
    }

    public static class EventGenerator {
        final int streamId;
        final int numKeys;
        final long startTime;

        int currentKey;

        public EventGenerator(int streamId, int numKeys, long startTime) {
            this.streamId = streamId;
            this.numKeys = numKeys;
            this.startTime = startTime;
            this.currentKey = 0;
        }

        public List<String> getNextWindow(long realTime) {
            currentKey = (currentKey + 1) % numKeys;
            final long eventTimestamp = realTime - startTime;
            return List.of(new Event(realTime, eventTimestamp, streamId, currentKey).asString());
        }
    }
}
