package com.github.lawben.disco.utils;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;


public class DistributiveWindowMatcher extends TypeSafeMatcher<List<String>> {
    private final ExpectedDistributiveWindow expectedWindow;

    public static DistributiveWindowMatcher equalsWindow(final ExpectedDistributiveWindow expectedWindow) {
        return new DistributiveWindowMatcher(expectedWindow);
    }

    private DistributiveWindowMatcher(ExpectedDistributiveWindow expectedWindow) {
        this.expectedWindow = expectedWindow;
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        if (windowString.size() != 4) return false;
        if (!String.valueOf(expectedWindow.getChildId()).equals(windowString.get(0))) return false;

        FunctionWindowAggregateId functionWindowId = expectedWindow.getFunctionWindowAggregateId();
        String expectedWindowString = DistributedUtils.childlessFunctionWindowIdToString(functionWindowId);

        if (!expectedWindowString.equals(windowString.get(1))) return false;
        if (!DistributedUtils.DISTRIBUTIVE_STRING.equals(windowString.get(2))) return false;
        return String.valueOf(expectedWindow.getValue()).equals(windowString.get(3));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches window=`" + expectedWindow + "`");
    }
}
