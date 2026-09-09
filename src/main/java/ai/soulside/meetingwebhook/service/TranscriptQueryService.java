package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.model.SegmentResponse;
import ai.soulside.meetingwebhook.api.SessionNotFoundException;
import ai.soulside.meetingwebhook.model.TranscriptResponse;
import ai.soulside.meetingwebhook.repository.SessionRepository;
import ai.soulside.meetingwebhook.repository.TranscriptSegmentRepository;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TranscriptQueryService {
    private final SessionRepository sessions;
    private final TranscriptSegmentRepository segments;

    public TranscriptQueryService(
            SessionRepository sessions,
            TranscriptSegmentRepository segments) {
        this.sessions = sessions;
        this.segments = segments;
    }

    @Transactional(readOnly = true)
    public TranscriptResponse getTranscript(String meetingId, String sessionId) {
        var session = sessions.findById(sessionId)
                .orElseThrow(() -> new SessionNotFoundException("Session not found"));

        if (!session.getMeeting().getId().equals(meetingId)) {
            throw new SessionNotFoundException("Session not found for meeting");
        }

        List<SegmentResponse> transcriptSegments = segments
                .findBySessionSessionIdOrderBySequenceNumberAsc(sessionId)
                .stream()
                .map(segment -> new SegmentResponse(
                        segment.getSequenceNumber(),
                        segment.getSpeaker(),
                        segment.getContent(),
                        segment.getStartOffset(),
                        segment.getEndOffset()))
                .toList();

        return new TranscriptResponse(
                meetingId,
                sessionId,
                session.getFullTranscript(),
                transcriptSegments);
    }
}