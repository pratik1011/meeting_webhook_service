package ai.soulside.meetingwebhook.service;

import ai.soulside.meetingwebhook.domain.entity.BufferedWebhookEvent;
import ai.soulside.meetingwebhook.domain.enums.BufferedWebhookEventStatus;
import ai.soulside.meetingwebhook.repository.BufferedWebhookEventRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class PendingEventIndex {
    private static final String KEY_PREFIX = "webhook:pending:";

    private final BufferedWebhookEventRepository events;
    private final ObjectProvider<StringRedisTemplate> redisTemplateProvider;
    private final boolean redisEnabled;

    public PendingEventIndex(
            BufferedWebhookEventRepository events,
            ObjectProvider<StringRedisTemplate> redisTemplateProvider,
            @Value("${webhook.redis.enabled:false}") boolean redisEnabled) {
        this.events = events;
        this.redisTemplateProvider = redisTemplateProvider;
        this.redisEnabled = redisEnabled;
    }

    public void add(BufferedWebhookEvent event) {
        withRedis(template -> template.opsForZSet().add(
                key(event.getMeetingId()), event.getId().toString(), event.getReceivedAt().toEpochMilli()));
    }

    public void remove(BufferedWebhookEvent event) {
        withRedis(template -> template.opsForZSet().remove(key(event.getMeetingId()), event.getId().toString()));
    }

    public long pendingCount(String meetingId) {
        StringRedisTemplate template = redisTemplate();
        if (template != null) {
            try {
                String key = key(meetingId);
                if (Boolean.TRUE.equals(template.hasKey(key))) {
                    Long count = template.opsForZSet().zCard(key);
                    return count == null ? 0 : count;
                }
            } catch (RuntimeException ignored) {
                // PostgreSQL is the durable fallback if Redis is unavailable.
            }
        }
        return events.countByMeetingIdAndStatus(meetingId, BufferedWebhookEventStatus.PENDING);
    }

    private void withRedis(java.util.function.Consumer<StringRedisTemplate> action) {
        StringRedisTemplate template = redisTemplate();
        if (template == null) {
            return;
        }
        try {
            action.accept(template);
        } catch (RuntimeException ignored) {
            // The event remains safely stored in PostgreSQL.
        }
    }

    private StringRedisTemplate redisTemplate() {
        return redisEnabled ? redisTemplateProvider.getIfAvailable() : null;
    }

    private String key(String meetingId) {
        return KEY_PREFIX + meetingId;
    }
}