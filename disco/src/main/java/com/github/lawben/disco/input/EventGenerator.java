package com.github.lawben.disco.input;

import org.zeromq.ZMQ;

public interface EventGenerator {
    long generateAndSendEvents(ZMQ.Socket eventSender) throws Exception;
}
