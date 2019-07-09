package com.github.lawben.disco.utils;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;


public class DistributiveWindowMatcher extends BaseWindowMatcher<ExpectedDistributiveWindow> {
    public static DistributiveWindowMatcher equalsWindow(final ExpectedDistributiveWindow expectedWindow) {
        return new DistributiveWindowMatcher(expectedWindow);
    }

    private DistributiveWindowMatcher(ExpectedDistributiveWindow expectedWindow) {
        super(expectedWindow);
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        if (!matchesCommon(windowString)) return false;
        if (!DistributedUtils.DISTRIBUTIVE_STRING.equals(windowString.get(3))) return false;
        return String.valueOf(expectedWindow.getValue()).equals(windowString.get(4));
    }
}
