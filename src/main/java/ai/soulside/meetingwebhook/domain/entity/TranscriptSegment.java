package ai.soulside.meetingwebhook.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(indexes = @Index(name = "idx_segment_session_sequence", columnList = "session_id,sequenceNumber"))
public class TranscriptSegment {
    @Id
    private String transcriptId;

    @ManyToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "session_id")
    private Session session;

    private long sequenceNumber;
    private String speaker;

    @Column(length = 4000)
    private String content;

    private String startOffset;
    private String endOffset;

    protected TranscriptSegment() {
    }

    public TranscriptSegment(
            String transcriptId,
            Session session,
            long sequenceNumber,
            String speaker,
            String content,
            String startOffset,
            String endOffset) {
        this.transcriptId = transcriptId;
        this.session = session;
        this.sequenceNumber = sequenceNumber;
        this.speaker = speaker;
        this.content = content;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getSpeaker() {
        return speaker;
    }

    public String getContent() {
        return content;
    }

    public String getStartOffset() {
        return startOffset;
    }

    public String getEndOffset() {
        return endOffset;
    }
}
