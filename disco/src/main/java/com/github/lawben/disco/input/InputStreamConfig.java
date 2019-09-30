package com.github.lawben.disco.input;

import java.util.function.Function;

public class InputStreamConfig {
    public final int numEventsToSend;
    public final int minWaitTimeMillis;
    public final int maxWaitTimeMillis;
    public final long startTimestamp;

    public final Function<Long, Long> generatorFunction;

    public InputStreamConfig(int numEventsToSend, int minWaitTimeMillis, int maxWaitTimeMillis, long startTimestamp,
            Function<Long, Long> generatorFunction) {
        this.numEventsToSend = numEventsToSend;
        this.minWaitTimeMillis = minWaitTimeMillis;
        this.maxWaitTimeMillis = maxWaitTimeMillis;
        this.startTimestamp = startTimestamp;
        this.generatorFunction = generatorFunction;
    }

    @Override
    public String toString() {
        return "InputStreamConfig{" +
                "numEventsToSend=" + numEventsToSend +
                ", minWaitTimeMillis=" + minWaitTimeMillis +
                ", maxWaitTimeMillis=" + maxWaitTimeMillis +
                ", startTimestamp=" + startTimestamp +
                ", generatorFunction=" + generatorFunction +
                '}';
    }
}
