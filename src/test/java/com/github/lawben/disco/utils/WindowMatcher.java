package com.github.lawben.disco.utils;

import static com.github.lawben.disco.Event.NO_KEY;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.BaseWindowAggregate;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import java.util.Arrays;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class WindowMatcher extends TypeSafeMatcher<List<String>> {
    private final int NUM_BASE_VALUES = 3;
    private final ExpectedWindow expectedWindow;

    private WindowMatcher(ExpectedWindow expectedWindow) {
        this.expectedWindow = expectedWindow;
    }

    public static WindowMatcher equalsWindow(ExpectedWindow expectedWindow) {
        return new WindowMatcher(expectedWindow);
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        List<BaseWindowAggregate> expectedWindowAggregates = expectedWindow.getExpectedWindowAggregates();
        if (windowString.size() != NUM_BASE_VALUES + expectedWindowAggregates.size()) return false;
        if (expectedWindowAggregates.isEmpty()) {
            return true;
        }

        if (!String.valueOf(expectedWindow.getChildId()).equals(windowString.get(0))) return false;
        if (!functionWindowIdStringsMatch(expectedWindow.getFunctionWindowId(), windowString.get(1))) return false;

        for (int windowNum = 0; windowNum < expectedWindowAggregates.size(); windowNum++) {
            BaseWindowAggregate expectedWindowAggregate = expectedWindowAggregates.get(windowNum);
            String actualWindow = windowString.get(NUM_BASE_VALUES + windowNum);
            if (!expectedWindowAggregate.equalsString(actualWindow)) return false;
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches window=" + expectedWindow);
    }

    public static boolean functionWindowIdStringsMatch(FunctionWindowAggregateId functionWindowId, String windowString) {
        String expectedWindowString = DistributedUtils.functionWindowIdToString(functionWindowId);
        if (functionWindowId.getKey() == NO_KEY) {
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
