package ai.soulside.meetingwebhook.repository;

import ai.soulside.meetingwebhook.domain.entity.Meeting;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MeetingRepository extends JpaRepository<Meeting, String> {
}
