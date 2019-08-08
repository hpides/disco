package com.github.lawben.disco.input;

import java.util.Random;
import java.util.function.Function;

public class InputStreamConfig<T> {
    public final int numEventsToSend;
    public final int minWaitTimeMillis;
    public final int maxWaitTimeMillis;
    public final long startTimestamp;

    public final Function<Random, T> generatorFunction;
    public final long randomSeed;

    public InputStreamConfig(int numEventsToSend, int minWaitTimeMillis, int maxWaitTimeMillis, long startTimestamp,
            Function<Random, T> generatorFunction, long randomSeed) {
        this.numEventsToSend = numEventsToSend;
        this.minWaitTimeMillis = minWaitTimeMillis;
        this.maxWaitTimeMillis = maxWaitTimeMillis;
        this.startTimestamp = startTimestamp;
        this.generatorFunction = generatorFunction;
        this.randomSeed = randomSeed;
    }

    @Override
    public String toString() {
        return "InputStreamConfig{" +
                "numEventsToSend=" + numEventsToSend +
                ", minWaitTimeMillis=" + minWaitTimeMillis +
                ", maxWaitTimeMillis=" + maxWaitTimeMillis +
                ", startTimestamp=" + startTimestamp +
                ", generatorFunction=" + generatorFunction +
                ", randomSeed=" + randomSeed +
                '}';
    }
}
