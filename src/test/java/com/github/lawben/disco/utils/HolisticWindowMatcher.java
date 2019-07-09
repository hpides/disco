package com.github.lawben.disco.utils;

import static com.github.lawben.disco.DistributedUtils.slicesToString;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.DistributedSlice;
import java.util.List;


public class HolisticWindowMatcher extends BaseWindowMatcher<ExpectedHolisticWindow> {
    public static HolisticWindowMatcher equalsWindow(final ExpectedHolisticWindow expectedWindow) {
        return new HolisticWindowMatcher(expectedWindow);
    }

    private HolisticWindowMatcher(ExpectedHolisticWindow expectedWindow) {
        super(expectedWindow);
    }

    @Override
    protected boolean matchesSafely(List<String> windowString) {
        if (!matchesCommon(windowString)) return false;
        if (!DistributedUtils.HOLISTIC_STRING.equals(windowString.get(3))) return false;
        List<DistributedSlice> expectedSlices = expectedWindow.getValue();
        String expectedSliceString = slicesToString(expectedSlices);
        return expectedSliceString.equals(windowString.get(4));
    }
}
