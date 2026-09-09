package ai.soulside.meetingwebhook.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class BatchExecutionConfiguration {
    @Bean("webhookBatchExecutor")
    TaskExecutor webhookBatchExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(1_000);
        executor.setThreadNamePrefix("webhook-batch-");
        executor.initialize();
        return executor;
    }

    @Bean("webhookBatchScheduler")
    TaskScheduler webhookBatchScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("webhook-batch-timer-");
        scheduler.initialize();
        return scheduler;
    }
}