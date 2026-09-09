package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.redis.PendingEventIndex;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.event.MeetingWebhookReceived;
import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "webhook.kafka.enabled", havingValue = "false", matchIfMissing = true)
public class WebhookBatchConsumer implements BufferedWebhookBatchHandler {
    private static final Logger log = LoggerFactory.getLogger(WebhookBatchConsumer.class);

    private final BufferedWebhookEventRepository events;
    private final WebhookEventBuffer buffer;
    private final WebhookEventProcessor processor;
    private final PendingEventIndex pendingEventIndex;

    public WebhookBatchConsumer(
            BufferedWebhookEventRepository events,
            WebhookEventBuffer buffer,
            WebhookEventProcessor processor,
            PendingEventIndex pendingEventIndex) {
        this.events = events;
        this.buffer = buffer;
        this.processor = processor;
        this.pendingEventIndex = pendingEventIndex;
    }

    @Override
    public void handle(List<BufferedWebhookEvent> meetingBatch) {
        for (BufferedWebhookEvent event : meetingBatch) {
            try {
                processor.process(new MeetingWebhookReceived(buffer.deserialize(event)));
                events.delete(event);
                pendingEventIndex.remove(event);
            } catch (RuntimeException exception) {
                event.markFailed(exception.getMessage());
                events.save(event);
                pendingEventIndex.remove(event);
                log.error("Unable to process buffered webhook event for meeting {}", event.getMeetingId(), exception);
            }
        }
    }
}