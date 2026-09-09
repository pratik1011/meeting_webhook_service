package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.model.WebhookPayload;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "false", matchIfMissing = true)
public class DatabaseWebhookIngress implements WebhookIngress {
    private final WebhookEventBuffer buffer;

    public DatabaseWebhookIngress(WebhookEventBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void accept(WebhookPayload payload) {
        buffer.enqueue(payload);
    }
}