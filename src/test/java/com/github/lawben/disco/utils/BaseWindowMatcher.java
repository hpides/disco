package com.github.lawben.disco.utils;

import static com.github.lawben.disco.DistributedUtils.WINDOW_COMPLETE;
import static com.github.lawben.disco.DistributedUtils.WINDOW_PARTIAL;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.Arrays;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public abstract class BaseWindowMatcher<WindowType extends ExpectedWindow> extends TypeSafeMatcher<List<String>> {
    protected final WindowType expectedWindow;

    protected BaseWindowMatcher(WindowType expectedWindow) {
        this.expectedWindow = expectedWindow;
    }

    protected boolean matchesCommon(List<String> windowString) {
        if (windowString.size() != 5) return false;
        if (!String.valueOf(expectedWindow.getChildId()).equals(windowString.get(0))) return false;
        if (!functionWindowIdStringsMatch(expectedWindow.getFunctionWindowAggregateId(), windowString.get(1))) return false;

        String expectedComplete = expectedWindow.windowIsComplete() ? WINDOW_COMPLETE : WINDOW_PARTIAL;
        return expectedComplete.equals(windowString.get(2));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches window=`" + expectedWindow + "`");
    }

    public static boolean functionWindowIdStringsMatch(FunctionWindowAggregateId functionWindowId, String windowString) {
        String expectedWindowString = DistributedUtils.functionWindowIdToString(functionWindowId);
        if (functionWindowId.getKey() == FunctionWindowAggregateId.NO_KEY) {
            // Ignore stream id
            int endIndex = 5;
            if (functionWindowId.getChildId() == FunctionWindowAggregateId.NO_CHILD_ID) {
                // Also ignore child id
                endIndex = 4;
            }
            String[] expectedParts = expectedWindowString.split(",");
            String expectedChildlessWindowString = String.join(",", Arrays.copyOfRange(expectedParts, 0, endIndex));

            String[] actualParts = windowString.split(",");
            String actualChildlessWindowString = String.join(",", Arrays.copyOfRange(actualParts, 0, endIndex));
            return expectedChildlessWindowString.equals(actualChildlessWindowString);
        }

        return expectedWindowString.equals(windowString);
    }
}
