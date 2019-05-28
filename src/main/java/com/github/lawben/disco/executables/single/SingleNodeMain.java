package com.github.lawben.disco.executables.single;

import com.github.lawben.disco.ResultListener;
import com.github.lawben.disco.single.SingleNode;

public class SingleNodeMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Not enough arguments!\nUsage: java ... rootPort resultPath");
            System.exit(1);
        }

        final int rootPort = Integer.parseInt(args[0]);
        final String resultPath = args[1];

        runSingleNode(rootPort, resultPath);
    }

    public static Thread runSingleNode(int rootPort, String resultPath) {
        ResultListener resultListener = new ResultListener(resultPath);
        Thread resultThread = new Thread(resultListener);
        resultThread.start();

        SingleNode worker = new SingleNode(rootPort, resultPath);
        Thread thread = new Thread(worker);
        thread.start();
        return thread;
    }
}
