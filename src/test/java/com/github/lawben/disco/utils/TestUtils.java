package com.github.lawben.disco.utils;

import java.io.IOException;
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
}
