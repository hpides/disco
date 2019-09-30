package com.github.lawben.disco.executables;

import com.github.lawben.disco.ResultListener;
import com.github.lawben.disco.SingleNodeRoot;

public class SingleNodeRootMain {
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Not enough arguments!\nUsage: java ... controllerPort windowPort resultPath "
                                                                    + "numChildren windowsString aggFnsString");
            System.exit(1);
        }

        final int rootControllerPort = Integer.parseInt(args[0]);
        final int rootWindowPort = Integer.parseInt(args[1]);
        final String rootResultPath = args[2];
        final int numChildren = Integer.parseInt(args[3]);
        final String windowsString = args[4];
        final String aggFnsString = args[5];

        runSingleNode(rootControllerPort, rootWindowPort, rootResultPath, numChildren, windowsString, aggFnsString);
    }

    public static Thread runSingleNode(int controllerPort, int windowPort, String resultPath, int numChildren,
                                       String windowsString, String aggFnsString) {
        ResultListener resultListener = new ResultListener(resultPath);
        Thread resultThread = new Thread(resultListener);
        resultThread.start();

        SingleNodeRoot worker = new SingleNodeRoot(controllerPort, windowPort, resultPath, numChildren,
                                                   windowsString, aggFnsString);
        Thread rootThread = new Thread(worker);
        rootThread.start();
        return rootThread;
    }
}
