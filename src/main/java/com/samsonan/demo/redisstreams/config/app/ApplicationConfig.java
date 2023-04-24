package com.samsonan.demo.redisstreams.config.app;

import com.samsonan.demo.redisstreams.config.redis.RedisStreamConsumerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
public class ApplicationConfig {

    @Bean
    @ConfigurationProperties(prefix = "cache.consumer")
    public RedisStreamConsumerProperties consumerConfig() {
        return new RedisStreamConsumerProperties();
    }

    @Bean
    @Daemon
    public ThreadPoolTaskScheduler taskScheduler() {
        var taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setDaemon(true);
        taskScheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        taskScheduler.setRemoveOnCancelPolicy(true);
        return taskScheduler;
    }
}
