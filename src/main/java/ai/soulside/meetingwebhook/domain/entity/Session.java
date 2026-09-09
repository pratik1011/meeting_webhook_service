package ai.soulside.meetingwebhook.domain.entity;

import ai.soulside.meetingwebhook.domain.enums.SessionStatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "meeting_sessions")
public class Session {
    @Id
    private String sessionId;

    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "meeting_id")
    private Meeting meeting;

    @Enumerated(EnumType.STRING)
    private SessionStatus status;

    private Instant startedAt;
    private Instant endedAt;

    @Column(name = "full_transcript", length = 100_000)
    private String fullTranscript = "";

    protected Session() {
    }

    public Session(String sessionId, Meeting meeting, Instant startedAt) {
        this.sessionId = sessionId;
        this.meeting = meeting;
        this.startedAt = startedAt;
        this.status = SessionStatus.LIVE;
    }

    public void end(Instant endedAt) {
        this.status = SessionStatus.ENDED;
        this.endedAt = endedAt;
    }

    public void updateFullTranscript(String fullTranscript) {
        this.fullTranscript = fullTranscript;
    }

    public String getFullTranscript() {
        return fullTranscript;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Meeting getMeeting() {
        return meeting;
    }

    public SessionStatus getStatus() {
        return status;
    }
}
