package com.github.lawben.disco.utils;

import static com.github.lawben.disco.DistributedUtils.CONTROL_STRING;
import static com.github.lawben.disco.DistributedUtils.functionWindowIdToString;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class SessionControlMatcher extends TypeSafeMatcher<List<String>> {
    private final FunctionWindowAggregateId expectedSessionStart;

    public static SessionControlMatcher equalsSessionStart(FunctionWindowAggregateId expectedSessionStart) {
        return new SessionControlMatcher(expectedSessionStart);
    }

    private SessionControlMatcher(FunctionWindowAggregateId expectedSessionStart) {
        this.expectedSessionStart = expectedSessionStart;
    }

    @Override
    protected boolean matchesSafely(List<String> eventString) {
        if (eventString.size() != 2) return false;
        if (!eventString.get(0).equals(CONTROL_STRING)) return false;
        return WindowMatcher.functionWindowIdStringsMatch(expectedSessionStart, eventString.get(1));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches session start=`" + expectedSessionStart + "`");
    }
}
