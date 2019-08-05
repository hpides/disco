package com.github.lawben.disco.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.Utils;

public class TestUtils {

    public static void closeIfNotNull(AutoCloseable x) throws Exception {
        if (x != null) {
            x.close();
        }
    }

    public static int findChildPort() throws IOException {
        int childPort;
        while ((childPort = Utils.findOpenPort()) > 65536 - 100) {}
        return childPort;
    }

    public static List<String> receiveResult(ZMQPullMock resultListener) {
        return resultListener.receiveNext(2);
    }

    public static List<List<String>> receiveWindows(int numExpectedWindows, ZMQPullMock resultListener) {
        List<List<String>> windowStrings = new ArrayList<>(numExpectedWindows);
        for (int i = 0; i < numExpectedWindows; i++) {
            windowStrings.add(receiveResult(resultListener));
        }
        return windowStrings;
    }
}
