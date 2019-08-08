package com.github.lawben.disco;

public class DistributedMergeNode implements Runnable {
    private final static String NODE_IDENTIFIER = "NODE";

    private final DistributedChildNode childNodeImpl;
    private final DistributedParentNode parentNodeImpl;

    public DistributedMergeNode(String parentIp, int parentControllerPort, int parentWindowPort,
            int controllerPort, int windowPort, int numChildren, int nodeId) {
        this.childNodeImpl = new DistributedChildNode(nodeId, NODE_IDENTIFIER, parentIp, parentControllerPort, parentWindowPort);
        this.parentNodeImpl = new DistributedParentNode(nodeId, NODE_IDENTIFIER, controllerPort, windowPort, numChildren);

    }

    @Override
    public void run() {
        childNodeImpl.registerAtParent();
        parentNodeImpl.waitForChildren();
        processPreAggregatedWindows();
    }

    private void processPreAggregatedWindows() {

    }
}
