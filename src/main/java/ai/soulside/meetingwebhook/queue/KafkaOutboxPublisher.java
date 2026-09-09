package ai.soulside.meetingwebhook.queue;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.model.WebhookPayload;
import ai.soulside.meetingwebhook.redis.PendingEventIndex;
import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import ai.soulside.meetingwebhook.service.BufferedWebhookBatchHandler;
import ai.soulside.meetingwebhook.service.WebhookEventBuffer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "true")
public class KafkaOutboxPublisher implements BufferedWebhookBatchHandler {
    private static final Logger log = LoggerFactory.getLogger(KafkaOutboxPublisher.class);

    private final BufferedWebhookEventRepository events;
    private final PendingEventIndex pendingEventIndex;
    private final WebhookEventBuffer buffer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String batchTopic;

    public KafkaOutboxPublisher(
            BufferedWebhookEventRepository events,
            PendingEventIndex pendingEventIndex,
            WebhookEventBuffer buffer,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            Environment environment) {
        this.events = events;
        this.pendingEventIndex = pendingEventIndex;
        this.buffer = buffer;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.batchTopic = environment.getProperty("webhook.kafka.batch-topic", "meeting-transcript-batches");
    }

    @Override
    @Transactional
    public void handle(List<BufferedWebhookEvent> meetingBatch) {
        String meetingId = meetingBatch.get(0).getMeetingId();
        String batchId = stableBatchId(meetingBatch);
        try {
            List<WebhookPayload> payloads = meetingBatch.stream().map(buffer::deserialize).toList();
            MeetingTranscriptBatch batch = new MeetingTranscriptBatch(batchId, meetingId, payloads);
            kafkaTemplate.send(batchTopic, meetingId, objectMapper.writeValueAsString(batch)).get();
            for (BufferedWebhookEvent event : meetingBatch) {
                events.delete(event);
                pendingEventIndex.remove(event);
            }
            log.info("Published transcript batch: batchId={}, meetingId={}, eventCount={}",
                    batchId, meetingId, meetingBatch.size());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize transcript batch", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Kafka transcript-batch publish failed", exception);
        }
    }

    private String stableBatchId(List<BufferedWebhookEvent> meetingBatch) {
        String eventIds = meetingBatch.stream()
                .map(event -> event.getId().toString())
                .collect(Collectors.joining(","));
        return UUID.nameUUIDFromBytes(eventIds.getBytes(StandardCharsets.UTF_8)).toString();
    }
}