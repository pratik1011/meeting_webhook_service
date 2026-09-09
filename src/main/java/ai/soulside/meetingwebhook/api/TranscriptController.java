package ai.soulside.meetingwebhook.api;

import ai.soulside.meetingwebhook.model.TranscriptResponse;
import ai.soulside.meetingwebhook.service.TranscriptQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/meetings/{meetingId}/sessions/{sessionId}/transcript")
public class TranscriptController {
    private final TranscriptQueryService queryService;

    public TranscriptController(TranscriptQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public TranscriptResponse transcript(
            @PathVariable String meetingId,
            @PathVariable String sessionId) {
        return queryService.getTranscript(meetingId, sessionId);
    }
}