package ai.soulside.meetingwebhook.repository;

import ai.soulside.meetingwebhook.domain.entity.ProcessedKafkaBatch;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedKafkaBatchRepository extends JpaRepository<ProcessedKafkaBatch, String> {
}