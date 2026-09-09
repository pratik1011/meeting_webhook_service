package ai.soulside.meetingwebhook.model;

public record SegmentResponse(
        long sequenceNumber,
        String speaker,
        String content,
        String startOffset,
        String endOffset) {
}