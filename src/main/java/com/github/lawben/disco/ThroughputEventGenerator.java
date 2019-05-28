package com.github.lawben.disco;

import java.util.Random;

/**
 * Creates as many events as possible with natural time.
 */
final public class ThroughputEventGenerator<T> extends SleepEventGenerator<T> {

    public ThroughputEventGenerator(int streamId, InputStreamConfig<T> config) {
        super(streamId, config);
    }

    @Override
    final protected void doSleep(int minSleep, int maxSleep, Random rand) {
        // Do nothing
    }
}

