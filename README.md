# Meeting webhook service

A Java 17 / Spring Boot service that receives meeting lifecycle webhooks, persists them, and builds ordered meeting transcripts.

## Run locally with H2

```bash
mvn spring-boot:run
```

This uses the database-backed local batch worker. H2 is in-memory and resets when the application stops.

## Run with PostgreSQL and Kafka

Docker Desktop is required. Start the full local stack:

```bash
docker compose up --build
```

The application runs on `http://localhost:8080`; PostgreSQL is exposed on port `5432` and Kafka on port `29092`.

## API

`POST /api/webhooks` accepts `meeting.started`, `meeting.transcript`, and `meeting.ended`. Valid requests receive `202 Accepted` after their payload is stored in the `webhook_events` outbox table.

`GET /api/meetings/{meetingId}/sessions/{sessionId}/transcript` returns ordered segments and joined transcript text.

## Architecture

Every webhook is validated, serialized, and stored as a `PENDING` database outbox row. This makes acknowledgement independent of processing and preserves work across application restarts when PostgreSQL is used.

Without the `kafka` profile, a scheduled local worker reads up to 50 pending rows every 100 ms, groups them by session ID, and processes each session's messages in order.

With the `kafka` profile, `KafkaOutboxPublisher` publishes pending rows to the `meeting-webhooks` topic using `sessionId` as the Kafka key. Kafka therefore routes a session's events to one partition and maintains their order. `KafkaWebhookConsumer` reads the topic and invokes the transactional event processor. A successfully published outbox row is deleted; a temporary publish failure leaves the row pending for a later retry.

The event processor performs the domain work through Spring Data JPA:

```text
meeting.started    ? Meeting + LIVE Session
meeting.transcript ? TranscriptSegment
meeting.ended      ? Session status ENDED
```

## Trade-offs

- The local H2 profile is convenient but not durable across restarts.
- PostgreSQL plus Kafka provides durable buffering and per-session ordering, but a production-grade outbox would also add explicit delivery states, retry policies, dead-letter topics, and database row locking for multiple publisher instances.
- The current implementation retains optional integration tests, Docker Compose, Kafka, structured Spring logs, and the transcript read endpoint. Signature verification and API versioning remain intentionally out of scope.
