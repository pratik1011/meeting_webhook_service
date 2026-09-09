package ai.soulside.meetingwebhook.domain.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.Instant;

@Entity
public class Meeting {
    @Id
    private String id;

    private String title;
    private String roomName;
    private String organizedBy;
    private Instant createdAt;

    protected Meeting() {
    }

    public Meeting(String id, String title, String roomName, String organizedBy, Instant createdAt) {
        this.id = id;
        this.title = title;
        this.roomName = roomName;
        this.organizedBy = organizedBy;
        this.createdAt = createdAt;
    }

    public void update(String title, String roomName, String organizedBy, Instant createdAt) {
        this.title = title;
        this.roomName = roomName;
        this.organizedBy = organizedBy;
        this.createdAt = createdAt;
    }

    public String getId() {
        return id;
    }
}
