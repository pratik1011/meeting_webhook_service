package ai.soulside.meetingwebhook.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WebhookBatchPolicy {
    private final int sizeThreshold;
    private final long maxWaitMs;

    public WebhookBatchPolicy(
            @Value("${webhook.batch.size-threshold:10}") int sizeThreshold,
            @Value("${webhook.batch.max-wait-ms:100}") long maxWaitMs) {
        if (sizeThreshold < 1) {
            throw new IllegalArgumentException("webhook.batch.size-threshold must be at least 1");
        }
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("webhook.batch.max-wait-ms must not be negative");
        }
        this.sizeThreshold = sizeThreshold;
        this.maxWaitMs = maxWaitMs;
    }

    public boolean hasReachedSizeThreshold(long pendingCount) {
        return pendingCount >= sizeThreshold;
    }

    public long maxWaitMs() {
        return maxWaitMs;
    }
}