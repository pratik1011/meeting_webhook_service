package ai.soulside.meetingwebhook.queue;

import ai.soulside.meetingwebhook.model.WebhookPayload;
import ai.soulside.meetingwebhook.service.WebhookIngress;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "true")
public class KafkaWebhookIngressPublisher implements WebhookIngress {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String rawTopic;

    public KafkaWebhookIngressPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            Environment environment) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.rawTopic = environment.getProperty("webhook.kafka.raw-topic", "meeting-webhooks-raw");
    }

    @Override
    public void accept(WebhookPayload payload) {
        try {
            String serializedPayload = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(rawTopic, payload.meeting().id(), serializedPayload).get();
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize webhook payload", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to publish webhook to Kafka", exception);
        }
    }
}