package com.github.lawben.disco.input;

/**
 * Creates as many events as possible with natural time.
 */
final public class ThroughputEventGenerator extends SleepEventGenerator {

    public ThroughputEventGenerator(int streamId, InputStreamConfig config) {
        super(streamId, config);
    }

    @Override
    final protected void doSleep(int minSleep, int maxSleep) {
        // Do nothing
    }
}

