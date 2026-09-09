package ai.soulside.meetingwebhook.model;

import java.util.List;

public record TranscriptResponse(
        String meetingId,
        String sessionId,
        String transcript,
        List<SegmentResponse> segments) {
}