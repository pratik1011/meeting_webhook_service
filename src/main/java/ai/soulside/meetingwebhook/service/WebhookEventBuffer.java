package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.model.WebhookPayload;
import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.event.BufferedWebhookEventStored;
import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WebhookEventBuffer {
    private final BufferedWebhookEventRepository events;
    private final PendingEventIndex pendingEventIndex;
    private final ApplicationEventPublisher applicationEvents;
    private final ObjectMapper objectMapper;

    public WebhookEventBuffer(
            BufferedWebhookEventRepository events,
            PendingEventIndex pendingEventIndex,
            ApplicationEventPublisher applicationEvents,
            ObjectMapper objectMapper) {
        this.events = events;
        this.pendingEventIndex = pendingEventIndex;
        this.applicationEvents = applicationEvents;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void enqueue(WebhookPayload payload) {
        try {
            String serializedPayload = objectMapper.writeValueAsString(payload);
            BufferedWebhookEvent event = events.save(new BufferedWebhookEvent(
                    payload.meeting().id(),
                    payload.meeting().sessionId(),
                    payload.event(),
                    serializedPayload));
            pendingEventIndex.add(event);
            applicationEvents.publishEvent(new BufferedWebhookEventStored(
                    payload.meeting().id(), "meeting.ended".equals(payload.event())));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to store webhook payload", exception);
        }
    }

    public WebhookPayload deserialize(BufferedWebhookEvent event) {
        try {
            return objectMapper.readValue(event.getPayload(), WebhookPayload.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to read buffered webhook payload", exception);
        }
    }
}