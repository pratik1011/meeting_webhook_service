package ai.soulside.meetingwebhook.queue;

import ai.soulside.meetingwebhook.model.WebhookPayload;
import java.util.List;

public record MeetingTranscriptBatch(String batchId, String meetingId, List<WebhookPayload> events) {
}