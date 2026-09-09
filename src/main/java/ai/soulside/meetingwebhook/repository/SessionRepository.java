package ai.soulside.meetingwebhook.repository;

import ai.soulside.meetingwebhook.domain.entity.Session;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SessionRepository extends JpaRepository<Session, String> {
}
