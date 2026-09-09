package ai.soulside.meetingwebhook;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class MeetingWebhookApplication {
    public static void main(String[] args) {
        SpringApplication.run(MeetingWebhookApplication.class, args);
    }
}
