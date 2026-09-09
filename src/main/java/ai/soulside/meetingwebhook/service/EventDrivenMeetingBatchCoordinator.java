package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.domain.enums.BufferedWebhookEventStatus;
import ai.soulside.meetingwebhook.event.BufferedWebhookEventStored;
import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class EventDrivenMeetingBatchCoordinator {
    private final BufferedWebhookEventRepository events;
    private final BufferedWebhookBatchHandler batchHandler;
    private final WebhookBatchPolicy batchPolicy;
    private final PendingEventIndex pendingEventIndex;
    private final TaskExecutor executor;
    private final TaskScheduler scheduler;
    private final ConcurrentMap<String, ReentrantLock> meetingLocks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ScheduledFuture<?>> timers = new ConcurrentHashMap<>();

    public EventDrivenMeetingBatchCoordinator(
            BufferedWebhookEventRepository events,
            BufferedWebhookBatchHandler batchHandler,
            WebhookBatchPolicy batchPolicy,
            PendingEventIndex pendingEventIndex,
            @Qualifier("webhookBatchExecutor") TaskExecutor executor,
            @Qualifier("webhookBatchScheduler") TaskScheduler scheduler) {
        this.events = events;
        this.batchHandler = batchHandler;
        this.batchPolicy = batchPolicy;
        this.pendingEventIndex = pendingEventIndex;
        this.executor = executor;
        this.scheduler = scheduler;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onEventStored(BufferedWebhookEventStored stored) {
        long pendingCount = pendingEventIndex.pendingCount(stored.meetingId());
        if (stored.meetingEnded() || batchPolicy.hasReachedSizeThreshold(pendingCount)) {
            cancelTimer(stored.meetingId());
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
                batchHandler.handle(meetingBatch);
            }
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