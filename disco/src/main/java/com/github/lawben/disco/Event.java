package com.github.lawben.disco;

public class Event {
    public static final int NO_KEY = -2;

    private final long value;
    private final long timestamp;
    private final int streamId;
    private final int key;

    public Event(long value, long timestamp, int streamId) {
        this(value, timestamp, streamId, NO_KEY);
    }

    public Event(long value, long timestamp, int streamId, int key) {
        this.value = value;
        this.timestamp = timestamp;
        this.streamId = streamId;
        this.key = key;
    }

    public static Event fromString(String eventString) {
        final String[] eventParts = eventString.split(",");
        final int streamId = Integer.parseInt(eventParts[0]);
        final long eventTimestamp = Long.parseLong(eventParts[1]);
        final long eventValue = Long.parseLong(eventParts[2]);
        final int key = eventParts.length == 4 ? Integer.parseInt(eventParts[3]) : NO_KEY;
        return new Event(eventValue, eventTimestamp, streamId, key);
    }

    public long getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getStreamId() {
        return streamId;
    }

    public int getKey() {
        return key;
    }

    public String asString() {
        return streamId + "," + timestamp + "," + value + "," + key;
    }
}
