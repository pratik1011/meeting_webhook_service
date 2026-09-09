package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.domain.enums.BufferedWebhookEventStatus;
import ai.soulside.meetingwebhook.event.BufferedWebhookEventStored;
import ai.soulside.meetingwebhook.redis.PendingEventIndex;
import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class EventDrivenMeetingBatchCoordinator {
    private static final Logger log = LoggerFactory.getLogger(EventDrivenMeetingBatchCoordinator.class);

    private final BufferedWebhookEventRepository events;
    private final BufferedWebhookBatchHandler batchHandler;
    private final WebhookBatchPolicy batchPolicy;
    private final PendingEventIndex pendingEventIndex;
    private final TaskExecutor executor;
    private final TaskScheduler scheduler;
    private final long publishRetryDelayMs;
    private final ConcurrentMap<String, ReentrantLock> meetingLocks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ScheduledFuture<?>> timers = new ConcurrentHashMap<>();

    public EventDrivenMeetingBatchCoordinator(
            BufferedWebhookEventRepository events,
            BufferedWebhookBatchHandler batchHandler,
            WebhookBatchPolicy batchPolicy,
            PendingEventIndex pendingEventIndex,
            @Qualifier("webhookBatchExecutor") TaskExecutor executor,
            @Qualifier("webhookBatchScheduler") TaskScheduler scheduler,
            @Value("${webhook.batch.publish-retry-delay-ms:1000}") long publishRetryDelayMs) {
        this.events = events;
        this.batchHandler = batchHandler;
        this.batchPolicy = batchPolicy;
        this.pendingEventIndex = pendingEventIndex;
        this.executor = executor;
        this.scheduler = scheduler;
        this.publishRetryDelayMs = publishRetryDelayMs;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onEventStored(BufferedWebhookEventStored stored) {
        long pendingCount = pendingEventIndex.pendingCount(stored.meetingId());
        if (stored.meetingEnded() || batchPolicy.hasReachedSizeThreshold(pendingCount)) {
            cancelTimer(stored.meetingId());
            log.info("Submitting webhook batch: meetingId={}, pendingCount={}, reason={}",
                    stored.meetingId(), pendingCount, stored.meetingEnded() ? "meeting-ended" : "size-threshold");
            submitFlush(stored.meetingId());
            return;
        }
        scheduleTimeoutIfNeeded(stored.meetingId());
    }

    private void scheduleTimeoutIfNeeded(String meetingId) {
        timers.computeIfAbsent(meetingId, ignored -> scheduler.schedule(
                () -> {
                    timers.remove(meetingId);
                    submitFlush(meetingId);
                },
                Instant.now().plusMillis(batchPolicy.maxWaitMs())));
    }

    private void schedulePublishRetry(String meetingId) {
        timers.computeIfAbsent(meetingId, ignored -> scheduler.schedule(
                () -> {
                    timers.remove(meetingId);
                    submitFlush(meetingId);
                },
                Instant.now().plusMillis(publishRetryDelayMs)));
    }

    private void submitFlush(String meetingId) {
        executor.execute(() -> flushMeeting(meetingId));
    }

    private void flushMeeting(String meetingId) {
        ReentrantLock lock = meetingLocks.computeIfAbsent(meetingId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            cancelTimer(meetingId);
            List<BufferedWebhookEvent> meetingBatch = events
                    .findByMeetingIdAndStatusOrderByReceivedAtAscIdAsc(meetingId, BufferedWebhookEventStatus.PENDING);
            if (!meetingBatch.isEmpty()) {
                log.info("Dispatching webhook batch: meetingId={}, eventCount={}", meetingId, meetingBatch.size());
                batchHandler.handle(meetingBatch);
            }
        } catch (RuntimeException exception) {
            log.warn("Batch publish failed; retry scheduled: meetingId={}, delayMs={}",
                    meetingId, publishRetryDelayMs, exception);
            schedulePublishRetry(meetingId);
        } finally {
            lock.unlock();
            if (!lock.hasQueuedThreads()) {
                meetingLocks.remove(meetingId, lock);
            }
        }
    }

    private void cancelTimer(String meetingId) {
        ScheduledFuture<?> timer = timers.remove(meetingId);
        if (timer != null) {
            timer.cancel(false);
        }
    }
}