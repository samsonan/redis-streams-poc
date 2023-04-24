package com.samsonan.demo.redisstreams.config.redis;

import com.samsonan.demo.redisstreams.config.app.Daemon;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.TaskScheduler;

@Configuration
public class RedisStreamConfig {

    @Bean
    public RedisTemplate<String, Long> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        var redisTemplate = new RedisTemplate<String, Long>();

        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setDefaultSerializer(new Jackson2JsonRedisSerializer<>(Long.class));

        return redisTemplate;
    }

    @Bean // TODO just make RedisStreamConsumerManagingService a @service?
    public RedisStreamConsumerManagingService redisStreamConsumerService(StringRedisTemplate redisTemplate,
                                                                         RedisStreamConsumerProperties consumerConfig,
                                                                         @Daemon TaskScheduler taskScheduler) {
        return new RedisStreamConsumerManagingService(redisTemplate, consumerConfig, taskScheduler);
    }

    @Bean
    @Nullable
    public ConsumerMetadata consumerMetadata(RedisStreamConsumerProperties consumerProperties,
                                             RedisStreamConsumerManagingService consumerManagingService,
                                             StreamListener<String, MapRecord<String, String, String>> streamListener,
                                             RedisConnectionFactory redisConnectionFactory,
                                             RedisStreamListenerService listenerService,
                                             @Daemon TaskScheduler taskScheduler) {

        var consumerMetadata = consumerManagingService.manageConsumerAssignment();

        listenerService.launchRedisStreamListeningContainer(consumerManagingService, taskScheduler,
                consumerProperties, streamListener, redisConnectionFactory, consumerMetadata);

        return consumerMetadata;
    }
}
