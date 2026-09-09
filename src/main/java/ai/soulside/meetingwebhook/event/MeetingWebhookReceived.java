package ai.soulside.meetingwebhook.event;

import ai.soulside.meetingwebhook.model.WebhookPayload;

public record MeetingWebhookReceived(WebhookPayload payload) {
}
