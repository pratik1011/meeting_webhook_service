package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.model.WebhookPayload;

public interface WebhookIngress {
    void accept(WebhookPayload payload);
}