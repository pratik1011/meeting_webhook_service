package ai.soulside.meetingwebhook.domain.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "processed_kafka_batches")
public class ProcessedKafkaBatch {
    @Id
    private String batchId;

    private Instant processedAt;

    protected ProcessedKafkaBatch() {
    }

    public ProcessedKafkaBatch(String batchId) {
        this.batchId = batchId;
        this.processedAt = Instant.now();
    }
}