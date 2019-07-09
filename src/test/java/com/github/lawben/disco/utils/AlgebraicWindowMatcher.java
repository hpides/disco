package com.github.lawben.disco.utils;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;


public class AlgebraicWindowMatcher extends BaseWindowMatcher<ExpectedAlgebraicWindow> {
    public static AlgebraicWindowMatcher equalsWindow(final ExpectedAlgebraicWindow expectedWindow) {
        return new AlgebraicWindowMatcher(expectedWindow);
    }

    private AlgebraicWindowMatcher(ExpectedAlgebraicWindow expectedWindow) {
        super(expectedWindow);
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        if (!matchesCommon(windowString)) return false;
        if (!DistributedUtils.ALGEBRAIC_STRING.equals(windowString.get(3))) return false;
        return expectedWindow.getValue().asString().equals(windowString.get(4));
    }
}
