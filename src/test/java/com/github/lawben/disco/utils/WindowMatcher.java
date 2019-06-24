package com.github.lawben.disco.utils;

import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;


public class WindowMatcher extends TypeSafeMatcher<List<String>> {

    private final ExpectedWindow expectedWindow;

    public static WindowMatcher equalsWindow(final ExpectedWindow expectedWindow) {
        return new WindowMatcher(expectedWindow);
    }

    private WindowMatcher(ExpectedWindow expectedWindow) {
        this.expectedWindow = expectedWindow;
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        if (windowString.size() != 3) return false;
        if (!String.valueOf(expectedWindow.getChildId()).equals(windowString.get(0))) return false;

        WindowAggregateId windowId = expectedWindow.getWindowAggregateId();
        String expectedWindowString = windowId.getWindowId() + "," + windowId.getWindowStartTimestamp() +
                "," + windowId.getWindowEndTimestamp();

        if (!expectedWindowString.equals(windowString.get(1))) return false;
        return String.valueOf(expectedWindow.getValue()).equals(windowString.get(2));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches window=`" + expectedWindow + "`");
    }
}
