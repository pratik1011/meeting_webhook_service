package ai.soulside.meetingwebhook.repository;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.domain.enums.BufferedWebhookEventStatus;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BufferedWebhookEventRepository extends JpaRepository<BufferedWebhookEvent, Long> {
    List<BufferedWebhookEvent> findTop50ByStatusOrderByReceivedAtAscIdAsc(BufferedWebhookEventStatus status);

    List<BufferedWebhookEvent> findByMeetingIdAndStatusOrderByReceivedAtAscIdAsc(
            String meetingId, BufferedWebhookEventStatus status);

    long countByMeetingIdAndStatus(String meetingId, BufferedWebhookEventStatus status);

    long countByStatus(BufferedWebhookEventStatus status);

    long countByStatusAndSessionId(BufferedWebhookEventStatus status, String sessionId);
}
