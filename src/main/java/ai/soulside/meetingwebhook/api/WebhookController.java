package ai.soulside.meetingwebhook.api;

import ai.soulside.meetingwebhook.model.WebhookPayload;

import ai.soulside.meetingwebhook.service.WebhookIngress;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/webhooks")
public class WebhookController {
    private final WebhookIngress ingress;

    public WebhookController(WebhookIngress ingress) {
        this.ingress = ingress;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> receive(@Valid @RequestBody WebhookPayload payload) {
        validateEventShape(payload);
        ingress.accept(payload);
        return ResponseEntity.accepted().body(Map.of("status", "accepted"));
    }

    private void validateEventShape(WebhookPayload payload) {
        switch (payload.event()) {
            case "meeting.started" -> validateStartedEvent(payload);
            case "meeting.transcript" -> validateTranscriptEvent(payload);
            case "meeting.ended" -> validateEndedEvent(payload);
            default -> throw new InvalidWebhookException("Unsupported event: " + payload.event());
        }
    }

    private void validateStartedEvent(WebhookPayload payload) {
        if (payload.meeting().title() == null || payload.meeting().startedAt() == null) {
            throw new InvalidWebhookException(
                    "meeting.started requires meeting.title and meeting.startedAt");
        }
    }

    private void validateTranscriptEvent(WebhookPayload payload) {
        if (payload.data() == null) {
            throw new InvalidWebhookException("meeting.transcript requires data");
        }
    }

    private void validateEndedEvent(WebhookPayload payload) {
        if (payload.meeting().endedAt() == null) {
            throw new InvalidWebhookException("meeting.ended requires meeting.endedAt");
        }
    }
}