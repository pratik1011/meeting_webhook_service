package ai.soulside.meetingwebhook.kafka;

import ai.soulside.meetingwebhook.domain.entity.ProcessedKafkaBatch;
import ai.soulside.meetingwebhook.event.MeetingWebhookReceived;
import ai.soulside.meetingwebhook.repository.ProcessedKafkaBatchRepository;
import ai.soulside.meetingwebhook.service.WebhookEventProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "true")
public class KafkaTranscriptBatchConsumer {
    private final ObjectMapper objectMapper;
    private final ProcessedKafkaBatchRepository processedBatches;
    private final WebhookEventProcessor processor;

    public KafkaTranscriptBatchConsumer(
            ObjectMapper objectMapper,
            ProcessedKafkaBatchRepository processedBatches,
            WebhookEventProcessor processor) {
        this.objectMapper = objectMapper;
        this.processedBatches = processedBatches;
        this.processor = processor;
    }

    @KafkaListener(
            topics = "${webhook.kafka.batch-topic:meeting-transcript-batches}",
            groupId = "${webhook.kafka.batch-consumer-group:meeting-transcript-processor}")
    @Transactional
    public void consumeBatch(String serializedBatch) {
        try {
            process(objectMapper.readValue(serializedBatch, MeetingTranscriptBatch.class));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to read Kafka transcript batch", exception);
        }
    }

    @Transactional
    public void process(MeetingTranscriptBatch batch) {
        if (processedBatches.existsById(batch.batchId())) {
            return;
        }
        for (var event : batch.events()) {
            processor.process(new MeetingWebhookReceived(event));
        }
        processedBatches.save(new ProcessedKafkaBatch(batch.batchId()));
    }
}