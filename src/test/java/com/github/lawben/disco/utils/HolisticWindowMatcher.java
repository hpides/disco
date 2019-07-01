package com.github.lawben.disco.utils;

import com.github.lawben.disco.DistributedUtils;
import com.github.lawben.disco.aggregation.DistributedSlice;
import de.tub.dima.scotty.core.WindowAggregateId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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

        WindowAggregateId windowId = expectedWindow.getWindowAggregateId();
        String expectedWindowString = windowId.getWindowId() + "," + windowId.getWindowStartTimestamp() +
                "," + windowId.getWindowEndTimestamp();

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

    private String slicesToString(List<DistributedSlice> slices) {
        List<String> allSlices = new ArrayList<>();

        for (DistributedSlice slice : slices) {
            StringBuilder sb = new StringBuilder();
            sb.append(slice.getTStart());
            sb.append(',');
            sb.append(slice.getTEnd());
            sb.append(';');

            if (slice.getValues().isEmpty()) {
                allSlices.add(sb.toString());
                continue;
            }

            List<String> valueStrings = slice.getValues().stream().map(String::valueOf).collect(Collectors.toList());
            sb.append(String.join(",", valueStrings));
            allSlices.add(sb.toString());
        }

        return String.join("|", allSlices);
    }
}
