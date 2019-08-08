package com.github.lawben.disco.single;

import com.github.lawben.disco.input.EventGenerator;
import com.github.lawben.disco.input.InputStream;
import com.github.lawben.disco.input.InputStreamConfig;
import org.zeromq.ZContext;

public class SingleInputStream<T> extends InputStream<T> {
    public SingleInputStream(int streamId, InputStreamConfig<T> config, String nodeIp, int nodePort, EventGenerator<T> eventGenerator) {
        super(streamId, config, nodeIp, nodePort, eventGenerator);
    }

    @Override
    protected void registerAtNode(ZContext context) {
        // Do nothing
    }
}
