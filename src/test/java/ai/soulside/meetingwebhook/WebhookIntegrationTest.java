package ai.soulside.meetingwebhook;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import ai.soulside.meetingwebhook.repository.SessionRepository;
import ai.soulside.meetingwebhook.repository.TranscriptSegmentRepository;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {
        "webhook.kafka.enabled=true",
        "webhook.kafka.partitions=1",
        "webhook.kafka.raw-consumer-concurrency=1",
        "webhook.kafka.batch-consumer-concurrency=1",
        "webhook.kafka.consumer-retry-attempts=1",
        "webhook.kafka.consumer-retry-delay-ms=50",
        "webhook.batch.size-threshold=3",
        "webhook.batch.max-wait-ms=100"
})
@EmbeddedKafka(
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        partitions = 1,
        topics = {
                "meeting-webhooks-raw",
                "meeting-transcript-batches",
                "meeting-webhooks-raw.DLT",
                "meeting-transcript-batches.DLT"
        })
@AutoConfigureMockMvc
class WebhookIntegrationTest {
    private static final String BATCH_DLT_TOPIC = "meeting-transcript-batches.DLT";

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SessionRepository sessions;

    @Autowired
    private TranscriptSegmentRepository segments;

    @Autowired
    private BufferedWebhookEventRepository bufferedEvents;

    @BeforeEach
    void clearDatabase() {
        bufferedEvents.deleteAll();
        segments.deleteAll();
        sessions.deleteAll();
    }

    @Test
    void acceptsValidWebhookImmediately() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        mockMvc.perform(post("/api/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(startedPayload(meetingId, sessionId)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("accepted"));
    }

    @Test
    void processesAWaitingSessionAsSoonAsItReachesTheSizeThreshold() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        postWebhook(startedPayload(meetingId, sessionId));
        Thread.sleep(250);
        assertThat(sessions.existsById(sessionId)).isFalse();

        postWebhook(transcriptPayload(meetingId, sessionId, "threshold-one", 1, "First."));
        postWebhook(transcriptPayload(meetingId, sessionId, "threshold-two", 2, "Second."));

        awaitWithin(() -> segments.findBySessionSessionIdOrderBySequenceNumberAsc(sessionId).size() == 2, 750);
    }
    @Test
    void processesDifferentMeetingsIndependently() throws Exception {
        String firstMeetingId = UUID.randomUUID().toString();
        String secondMeetingId = UUID.randomUUID().toString();
        String firstSessionId = UUID.randomUUID().toString();
        String secondSessionId = UUID.randomUUID().toString();

        postWebhook(startedPayload(firstMeetingId, firstSessionId));
        postWebhook(transcriptPayload(firstMeetingId, firstSessionId, "first-one", 1, "One."));
        postWebhook(transcriptPayload(firstMeetingId, firstSessionId, "first-two", 2, "Two."));
        postWebhook(startedPayload(secondMeetingId, secondSessionId));
        postWebhook(transcriptPayload(secondMeetingId, secondSessionId, "second-one", 1, "One."));
        postWebhook(transcriptPayload(secondMeetingId, secondSessionId, "second-two", 2, "Two."));

        awaitWithin(() -> segments.findBySessionSessionIdOrderBySequenceNumberAsc(firstSessionId).size() == 2, 750);
        awaitWithin(() -> segments.findBySessionSessionIdOrderBySequenceNumberAsc(secondSessionId).size() == 2, 750);
    }
    @Test
    void rejectsUnsupportedWebhookEvents() throws Exception {
        mockMvc.perform(post("/api/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"event":"meeting.cancelled","meeting":{"id":"meeting-1","sessionId":"session-1"}}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid webhook payload"));
    }

    @Test
    void storesTranscriptInSequenceOrderAndEndsSession() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        postWebhook(startedPayload(meetingId, sessionId));
        postWebhook(transcriptPayload(meetingId, sessionId, "segment-two", 2, "Second sentence."));
        postWebhook(transcriptPayload(meetingId, sessionId, "segment-one", 1, "First sentence."));
        postWebhook(endedPayload(meetingId, sessionId));

        await(() -> sessions.findById(sessionId)
                .map(session -> session.getStatus().name().equals("ENDED"))
                .orElse(false));
        assertThat(segments.findBySessionSessionIdOrderBySequenceNumberAsc(sessionId)).hasSize(2);
        assertThat(sessions.findById(sessionId).orElseThrow().getFullTranscript())
                .isEqualTo("First sentence. Second sentence.");

        mockMvc.perform(get("/api/meetings/{meetingId}/sessions/{sessionId}/transcript", meetingId, sessionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transcript").value("First sentence. Second sentence."))
                .andExpect(jsonPath("$.segments[0].sequenceNumber").value(1))
                .andExpect(jsonPath("$.segments[1].sequenceNumber").value(2));
    }

    @Test
    void retainsOnlyOneTranscriptSegmentForDuplicateDelivery() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        postWebhook(startedPayload(meetingId, sessionId));
        postWebhook(transcriptPayload(meetingId, sessionId, "duplicate-segment", 1, "Only once."));
        postWebhook(transcriptPayload(meetingId, sessionId, "duplicate-segment", 1, "Only once."));
        postWebhook(endedPayload(meetingId, sessionId));

        await(() -> sessions.findById(sessionId)
                .map(session -> session.getStatus().name().equals("ENDED"))
                .orElse(false));
        assertThat(segments.findBySessionSessionIdOrderBySequenceNumberAsc(sessionId)).hasSize(1);
    }

    @Test
    void routesAnOutOfOrderTranscriptToTheBatchDeadLetterTopic() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        postWebhook(transcriptPayload(meetingId, sessionId, "early-segment", 1, "Too early."));
        awaitDeadLetterEvent(BATCH_DLT_TOPIC, sessionId);

        postWebhook(startedPayload(meetingId, sessionId));
        await(() -> sessions.existsById(sessionId));
        assertThat(segments.findBySessionSessionIdOrderBySequenceNumberAsc(sessionId)).isEmpty();
    }

    @Test
    void recordsTranscriptAfterEndAsFailed() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        postWebhook(startedPayload(meetingId, sessionId));
        postWebhook(endedPayload(meetingId, sessionId));
        postWebhook(transcriptPayload(meetingId, sessionId, "late-segment", 1, "Too late."));

        await(() -> sessions.findById(sessionId)
                .map(session -> session.getStatus().name().equals("ENDED"))
                .orElse(false));
        awaitDeadLetterEvent(BATCH_DLT_TOPIC, sessionId);
        assertThat(segments.findBySessionSessionIdOrderBySequenceNumberAsc(sessionId)).isEmpty();
    }

    @Test
    void recordsEndForUnknownSessionAsFailed() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String sessionId = UUID.randomUUID().toString();

        postWebhook(endedPayload(meetingId, sessionId));

        awaitDeadLetterEvent(BATCH_DLT_TOPIC, sessionId);
        assertThat(sessions.existsById(sessionId)).isFalse();
    }

    @Test
    void supportsConcurrentSessionsForTheSameMeeting() throws Exception {
        String meetingId = UUID.randomUUID().toString();
        String firstSessionId = UUID.randomUUID().toString();
        String secondSessionId = UUID.randomUUID().toString();

        postWebhook(startedPayload(meetingId, firstSessionId));
        postWebhook(startedPayload(meetingId, secondSessionId));
        postWebhook(transcriptPayload(meetingId, firstSessionId, "first-segment", 1, "First session."));
        postWebhook(transcriptPayload(meetingId, secondSessionId, "second-segment", 1, "Second session."));
        postWebhook(endedPayload(meetingId, firstSessionId));
        postWebhook(endedPayload(meetingId, secondSessionId));

        await(() -> sessions.findById(firstSessionId)
                .map(session -> session.getStatus().name().equals("ENDED"))
                .orElse(false));
        await(() -> sessions.findById(secondSessionId)
                .map(session -> session.getStatus().name().equals("ENDED"))
                .orElse(false));
        assertThat(segments.findBySessionSessionIdOrderBySequenceNumberAsc(firstSessionId)).hasSize(1);
        assertThat(segments.findBySessionSessionIdOrderBySequenceNumberAsc(secondSessionId)).hasSize(1);
    }
    private void postWebhook(String payload) throws Exception {
        mockMvc.perform(post("/api/webhooks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted());
    }

    private String startedPayload(String meetingId, String sessionId) {
        return """
                {
                  "event": "meeting.started",
                  "meeting": {
                    "id": "%s",
                    "sessionId": "%s",
                    "title": "Planning",
                    "roomName": "room-a",
                    "createdAt": "2024-12-13T06:57:09.736Z",
                    "startedAt": "2024-12-13T06:57:09.736Z",
                    "organizedBy": {"id": "person-1", "name": "Alice"}
                  }
                }
                """.formatted(meetingId, sessionId);
    }

    private String transcriptPayload(
            String meetingId,
            String sessionId,
            String transcriptId,
            int sequenceNumber,
            String content) {
        return """
                {
                  "event": "meeting.transcript",
                  "meeting": {"id": "%s", "sessionId": "%s"},
                  "data": {
                    "transcriptId": "%s",
                    "sequenceNumber": %d,
                    "speaker": {"id": "person-1", "name": "Alice"},
                    "content": "%s",
                    "startOffset": "00:00:01.000",
                    "endOffset": "00:00:02.000",
                    "language": "en"
                  }
                }
                """.formatted(meetingId, sessionId, transcriptId, sequenceNumber, content);
    }

    private String endedPayload(String meetingId, String sessionId) {
        return """
                {
                  "event": "meeting.ended",
                  "meeting": {
                    "id": "%s",
                    "sessionId": "%s",
                    "endedAt": "2024-12-13T07:04:37.052Z"
                  }
                }
                """.formatted(meetingId, sessionId);
    }

    private void awaitDeadLetterEvent(String topic, String sessionId) {
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                "dlt-assertion-" + UUID.randomUUID(), "false", kafkaBroker);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                consumerProperties, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                boolean found = consumer.poll(Duration.ofMillis(250)).records(topic).stream()
                        .anyMatch(record -> record.value().contains(sessionId));
                if (found) {
                    return;
                }
            }
        }
        throw new AssertionError("No dead-letter event found for session " + sessionId);
    }
    private void awaitWithin(Condition condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.met() && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        assertThat(condition.met()).isTrue();
    }
    private void await(Condition condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.met() && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        assertThat(condition.met()).isTrue();
    }

    @FunctionalInterface
    private interface Condition {
        boolean met();
    }
}
