package com.github.lawben.disco.executables;

import com.github.lawben.disco.ChildBenchmarkListener;

public class ChildBenchmarkListenerRunner {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Need 1 arg: streamPort");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);

        ChildBenchmarkListener childBenchmarkListener;
        if (args.length == 1) {
            childBenchmarkListener = new ChildBenchmarkListener(port);
        } else {
            int numStreams = Integer.parseInt(args[1]);
            childBenchmarkListener = new ChildBenchmarkListener(port, numStreams);
        }

        childBenchmarkListener.run();
    }
}
