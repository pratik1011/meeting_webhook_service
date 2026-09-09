package ai.soulside.meetingwebhook.api;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler({InvalidWebhookException.class, MethodArgumentNotValidException.class})
    ResponseEntity<Map<String, String>> invalidRequest(Exception exception) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "Invalid webhook payload",
                "message", exception.getMessage()));
    }

    @ExceptionHandler(SessionNotFoundException.class)
    ResponseEntity<Map<String, String>> notFound(SessionNotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", exception.getMessage()));
    }
}
