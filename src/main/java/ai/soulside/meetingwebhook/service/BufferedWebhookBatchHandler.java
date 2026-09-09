package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import java.util.List;

public interface BufferedWebhookBatchHandler {
    void handle(List<BufferedWebhookEvent> meetingBatch);
}