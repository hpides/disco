package com.github.lawben.disco.single;

import com.github.lawben.disco.input.EventGenerator;
import com.github.lawben.disco.input.InputStream;
import com.github.lawben.disco.input.InputStreamConfig;
import org.zeromq.ZContext;

public class SingleInputStream extends InputStream {
    public SingleInputStream(int streamId, InputStreamConfig config, String nodeIp, int nodePort, EventGenerator eventGenerator) {
        super(streamId, config, nodeIp, nodePort, eventGenerator);
    }

    @Override
    protected void registerAtNode(ZContext context) {
        // Do nothing
    }
}
