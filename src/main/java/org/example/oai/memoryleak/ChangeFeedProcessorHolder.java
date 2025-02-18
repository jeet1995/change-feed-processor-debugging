package org.example.oai.memoryleak;

import com.azure.cosmos.ChangeFeedProcessor;

public class ChangeFeedProcessorHolder {

    private final ChangeFeedProcessor processor;

    private final String targetFeedContainerName;

    private final String hostName;

    public ChangeFeedProcessorHolder(ChangeFeedProcessor processor, String targetFeedContainerName, String hostName) {
        this.processor = processor;
        this.targetFeedContainerName = targetFeedContainerName;
        this.hostName = hostName;
    }

    public ChangeFeedProcessor getChangeFeedProcessor() {
        return processor;
    }

    public String getHostName() {
        return hostName;
    }

    public String getTargetFeedContainerName() {
        return targetFeedContainerName;
    }
}
