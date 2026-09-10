package ai.soulside.meetingwebhook.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "true")
public class KafkaFailureHandlingConfiguration {
    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${webhook.kafka.consumer-retry-delay-ms:1000}") long retryDelayMs,
            @Value("${webhook.kafka.consumer-retry-attempts:3}") long retryAttempts) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(retryDelayMs, retryAttempts));
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    @Bean
    NewTopic rawWebhookTopic(
            @Value("${webhook.kafka.raw-topic:meeting-webhooks-raw}") String topic,
            @Value("${webhook.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic transcriptBatchTopic(
            @Value("${webhook.kafka.batch-topic:meeting-transcript-batches}") String topic,
            @Value("${webhook.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic rawWebhookDeadLetterTopic(
            @Value("${webhook.kafka.raw-topic:meeting-webhooks-raw}") String topic,
            @Value("${webhook.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(topic + ".DLT").partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic transcriptBatchDeadLetterTopic(
            @Value("${webhook.kafka.batch-topic:meeting-transcript-batches}") String topic,
            @Value("${webhook.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(topic + ".DLT").partitions(partitions).replicas(1).build();
    }
}