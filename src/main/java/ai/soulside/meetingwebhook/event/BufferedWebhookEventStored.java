package ai.soulside.meetingwebhook.event;

public record BufferedWebhookEventStored(String meetingId, boolean meetingEnded) {
}