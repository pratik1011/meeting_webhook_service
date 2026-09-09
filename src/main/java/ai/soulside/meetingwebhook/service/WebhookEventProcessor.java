package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.model.WebhookPayload;
import ai.soulside.meetingwebhook.domain.entity.Meeting;
import ai.soulside.meetingwebhook.domain.entity.Session;
import ai.soulside.meetingwebhook.domain.enums.SessionStatus;
import ai.soulside.meetingwebhook.domain.entity.TranscriptSegment;
import ai.soulside.meetingwebhook.event.MeetingWebhookReceived;
import ai.soulside.meetingwebhook.repository.MeetingRepository;
import ai.soulside.meetingwebhook.repository.SessionRepository;
import ai.soulside.meetingwebhook.repository.TranscriptSegmentRepository;
import java.time.Instant;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WebhookEventProcessor {
    private final MeetingRepository meetings;
    private final SessionRepository sessions;
    private final TranscriptSegmentRepository segments;

    public WebhookEventProcessor(
            MeetingRepository meetings,
            SessionRepository sessions,
            TranscriptSegmentRepository segments) {
        this.meetings = meetings;
        this.sessions = sessions;
        this.segments = segments;
    }

    @Transactional
    public void process(MeetingWebhookReceived received) {
        WebhookPayload payload = received.payload();

        switch (payload.event()) {
            case "meeting.started" -> start(payload);
            case "meeting.transcript" -> appendTranscript(payload);
            case "meeting.ended" -> end(payload);
            default -> throw new IllegalArgumentException("Unsupported event: " + payload.event());
        }
    }

    private void start(WebhookPayload payload) {
        var meetingData = payload.meeting();
        String organizer = meetingData.organizedBy() == null
                ? null
                : meetingData.organizedBy().name();

        Meeting meeting = meetings.findById(meetingData.id())
                .map(existing -> updateMeeting(existing, meetingData, organizer))
                .orElseGet(() -> new Meeting(
                        meetingData.id(),
                        meetingData.title(),
                        meetingData.roomName(),
                        organizer,
                        meetingData.createdAt()));
        meetings.save(meeting);

        if (!sessions.existsById(meetingData.sessionId())) {
            sessions.save(new Session(
                    meetingData.sessionId(),
                    meeting,
                    requiredTime(meetingData.startedAt(), "meeting.started.startedAt")));
        }
    }

    private Meeting updateMeeting(
            Meeting meeting,
            WebhookPayload.MeetingData meetingData,
            String organizer) {
        meeting.update(
                meetingData.title(),
                meetingData.roomName(),
                organizer,
                meetingData.createdAt());
        return meeting;
    }

    private void appendTranscript(WebhookPayload payload) {
        if (payload.data() == null) {
            throw new IllegalArgumentException("data is required for meeting.transcript");
        }

        Session session = sessions.findById(payload.meeting().sessionId())
                .orElseThrow(() -> new IllegalArgumentException("Session does not exist"));
        if (session.getStatus() != SessionStatus.LIVE) {
            throw new IllegalStateException("Cannot append a transcript to an ended session");
        }

        var transcriptData = payload.data();
        String speaker = transcriptData.speaker() == null ? null : transcriptData.speaker().name();
        segments.save(new TranscriptSegment(
                transcriptData.transcriptId(),
                session,
                transcriptData.sequenceNumber(),
                speaker,
                transcriptData.content(),
                transcriptData.startOffset(),
                transcriptData.endOffset()));
        session.updateFullTranscript(segments
                .findBySessionSessionIdOrderBySequenceNumberAsc(session.getSessionId())
                .stream()
                .map(TranscriptSegment::getContent)
                .collect(Collectors.joining(" ")));
    }

    private void end(WebhookPayload payload) {
        Session session = sessions.findById(payload.meeting().sessionId())
                .orElseThrow(() -> new IllegalArgumentException("Session does not exist"));
        session.end(requiredTime(payload.meeting().endedAt(), "meeting.ended.endedAt"));
        sessions.save(session);
    }

    private Instant requiredTime(Instant value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
