package ai.soulside.meetingwebhook.kafka;

import ai.soulside.meetingwebhook.model.WebhookPayload;
import ai.soulside.meetingwebhook.service.WebhookEventBuffer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "true")
public class KafkaWebhookConsumer {
    private final ObjectMapper objectMapper;
    private final WebhookEventBuffer buffer;

    public KafkaWebhookConsumer(ObjectMapper objectMapper, WebhookEventBuffer buffer) {
        this.objectMapper = objectMapper;
        this.buffer = buffer;
    }

    @KafkaListener(
            topics = "${webhook.kafka.raw-topic:meeting-webhooks-raw}",
            groupId = "${webhook.kafka.raw-consumer-group:meeting-webhook-inbox}")
    public void consumeRawEvent(String serializedPayload) {
        try {
            WebhookPayload payload = objectMapper.readValue(serializedPayload, WebhookPayload.class);
            buffer.enqueue(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to read Kafka webhook payload", exception);
        }
    }
}