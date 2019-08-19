package com.github.lawben.disco.utils;

import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class WindowResultMatcher extends TypeSafeMatcher<List<String>> {
    private final FunctionWindowAggregateId functionWindowId;
    private final Long aggregateValue;

    private WindowResultMatcher(FunctionWindowAggregateId functionWindowId, Long aggregateValue) {
        this.functionWindowId = functionWindowId;
        this.aggregateValue = aggregateValue;
    }

    public static WindowResultMatcher equalsWindowResult(FunctionWindowAggregateId functionWindowId, Long value) {
        return new WindowResultMatcher(functionWindowId, value);
    }

    @Override
    protected boolean matchesSafely(List<String> item) {
        if (item.size() != 2) return false;
        if (!WindowMatcher.functionWindowIdStringsMatch(functionWindowId, item.get(0))) return false;
        return Long.valueOf(item.get(1)).equals(aggregateValue);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches window result=" + functionWindowId + " with value: " + aggregateValue);
    }
}
