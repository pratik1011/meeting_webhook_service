package ai.soulside.meetingwebhook.repository;

import ai.soulside.meetingwebhook.domain.entity.TranscriptSegment;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TranscriptSegmentRepository extends JpaRepository<TranscriptSegment, String> {
    List<TranscriptSegment> findBySessionSessionIdOrderBySequenceNumberAsc(String sessionId);
}
