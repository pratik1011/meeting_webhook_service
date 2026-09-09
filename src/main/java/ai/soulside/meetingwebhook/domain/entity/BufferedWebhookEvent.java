package ai.soulside.meetingwebhook.domain.entity;

import ai.soulside.meetingwebhook.domain.enums.BufferedWebhookEventStatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "webhook_events")
public class BufferedWebhookEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String meetingId;
    private String sessionId;
    private String eventType;

    @Lob
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    private BufferedWebhookEventStatus status;

    private Instant receivedAt;

    @Column(length = 1000)
    private String failureReason;

    protected BufferedWebhookEvent() {
    }

    public BufferedWebhookEvent(String meetingId, String sessionId, String eventType, String payload) {
        this.meetingId = meetingId;
        this.sessionId = sessionId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = BufferedWebhookEventStatus.PENDING;
        this.receivedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getMeetingId() {
        return meetingId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void markFailed(String reason) {
        this.status = BufferedWebhookEventStatus.FAILED;
        this.failureReason = reason == null ? "Unknown processing error" : reason.substring(0, Math.min(reason.length(), 1000));
    }
}