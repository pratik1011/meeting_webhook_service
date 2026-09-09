package ai.soulside.meetingwebhook.api;

public class InvalidWebhookException extends RuntimeException {
    public InvalidWebhookException(String message) {
        super(message);
    }
}
