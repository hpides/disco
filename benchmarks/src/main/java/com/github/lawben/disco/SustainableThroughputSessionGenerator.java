package com.github.lawben.disco;


import com.github.lawben.disco.SustainableThroughputEventGenerator.EventGenerator;
import java.util.List;
import java.util.function.Function;

/**
 * Keeps queue of events and checks if back pressure is building up.
 * If the back pressure becomes to high, throws an exception.
 */
public class SustainableThroughputSessionGenerator extends SustainableThroughputGenerator {

    public static final int GAP_SIZE_MS = 2000;

    public static Function<Long, List<String>> getEventGen(final long startTime, final int streamId) {
//        if (streamId == 0) {
//            EventGenerator eventGenerator = new EventGenerator(streamId, 1, startTime);
//            return eventGenerator::getNextWindow;
//        } else {
            SessionGenerator sessionGenerator = new SessionGenerator(streamId, startTime);
            return sessionGenerator::getNextEvent;
//        }
    }

    public SustainableThroughputSessionGenerator(int streamId, int numEventsPerSecond, long startTimestamp) {
        super(numEventsPerSecond, getEventGen(startTimestamp, streamId));
    }

    private static class SessionGenerator {
        final int streamId;
        final long startTime;
        final int key;

        public SessionGenerator(int streamId, long startTime) {
            this.streamId = streamId;
            this.startTime = startTime;
            this.key = streamId;
        }

        public List<String> getNextEvent(long realTime) {
            final long realTimeInSeconds = realTime / 1000;
            if (realTimeInSeconds % 5 == 0) {
                // Create session gap
                try {
                    Thread.sleep(GAP_SIZE_MS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            final long eventTimestamp = realTime - startTime;
            return List.of(new Event(realTime, eventTimestamp, streamId, key).asString());
        }
    }
}
