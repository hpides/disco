package com.github.lawben.disco.utils;

import static com.github.lawben.disco.DistributedUtils.slicesToString;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.DistributedSlice;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;


public class HolisticWindowMatcher extends TypeSafeMatcher<List<String>> {
    private final ExpectedHolisticWindow expectedWindow;

    public static HolisticWindowMatcher equalsWindow(final ExpectedHolisticWindow expectedWindow) {
        return new HolisticWindowMatcher(expectedWindow);
    }

    private HolisticWindowMatcher(ExpectedHolisticWindow expectedWindow) {
        this.expectedWindow = expectedWindow;
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        if (windowString.size() != 4) return false;
        if (!String.valueOf(expectedWindow.getChildId()).equals(windowString.get(0))) return false;

        FunctionWindowAggregateId functionWindowId = expectedWindow.getFunctionWindowAggregateId();
        String expectedWindowString = DistributedUtils.childlessFunctionWindowIdToString(functionWindowId);

        if (!expectedWindowString.equals(windowString.get(1))) return false;
        if (!DistributedUtils.HOLISTIC_STRING.equals(windowString.get(2))) return false;

        List<DistributedSlice> expectedSlices = expectedWindow.getValue();
        String expectedSliceString = slicesToString(expectedSlices);
        return expectedSliceString.equals(windowString.get(3));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches window=`" + expectedWindow + "`");
    }
}
