package ai.soulside.meetingwebhook.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WebhookPayload(
        @NotBlank String event,
        @NotNull @Valid MeetingData meeting,
        @Valid TranscriptData data) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MeetingData(
            @NotBlank String id,
            @NotBlank String sessionId,
            String title,
            String roomName,
            Instant createdAt,
            Instant startedAt,
            Instant endedAt,
            @Valid Person organizedBy) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TranscriptData(
            @NotBlank String transcriptId,
            @PositiveOrZero Long sequenceNumber,
            @NotBlank String content,
            String startOffset,
            String endOffset,
            @Valid Person speaker) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Person(String id, @NotBlank String name) {
    }
}
