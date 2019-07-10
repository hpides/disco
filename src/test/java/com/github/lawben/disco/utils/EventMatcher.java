package com.github.lawben.disco.utils;

import static com.github.lawben.disco.DistributedUtils.EVENT_STRING;

import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class EventMatcher extends TypeSafeMatcher<List<String>> {
    private final String expectedEvent;

    public static EventMatcher equalsEvent(final String expectedEvent) {
        return new EventMatcher(expectedEvent);
    }

    private EventMatcher(String expectedEvent) {
        this.expectedEvent = expectedEvent;
    }

    @Override
    protected boolean matchesSafely(List<String> eventString) {
        if (eventString.size() != 2) return false;
        if (!eventString.get(0).equals(EVENT_STRING)) return false;
        return eventString.get(1).equals(expectedEvent);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches event=`" + expectedEvent + "`");
    }
}
