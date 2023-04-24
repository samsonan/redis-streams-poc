package com.samsonan.demo.redisstreams.config.redis;

import com.samsonan.demo.redisstreams.config.app.Daemon;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisStreamListenerService {

    private ScheduledFuture<?> scheduledFuture;

    private final ApplicationContext applicationContext;

    public void launchRedisStreamListeningContainer(RedisStreamConsumerManagingService managingService,
                                                    @Daemon TaskScheduler taskScheduler,
                                                    RedisStreamConsumerProperties consumerProperties,
                                                    StreamListener<String, MapRecord<String, String, String>> streamListener,
                                                    RedisConnectionFactory redisConnectionFactory,
                                                    ConsumerMetadata consumerMetadata) {
        log.debug("attempting to start redis stream consumer");

        if (consumerMetadata == null) {
            scheduleAssignmentWithDelay(managingService, taskScheduler,
                    consumerProperties, streamListener, redisConnectionFactory);
        } else {
            startListenerContainer(consumerProperties, consumerMetadata, managingService, streamListener, redisConnectionFactory);
        }
    }

    private void startListenerContainer(RedisStreamConsumerProperties consumerProperties,
                                        ConsumerMetadata consumerMetadata,
                                        RedisStreamConsumerManagingService managingService,
                                        StreamListener<String, MapRecord<String, String, String>> streamListener,
                                        RedisConnectionFactory redisConnectionFactory) {

        log.info("trying to add new consumer = [{}] to keyed stream = [{}]",
                consumerMetadata.getConsumerKey(),
                consumerMetadata.getStreamKey());

        configureAndStartContainer(consumerProperties.getGroup(),
                consumerMetadata,
                redisConnectionFactory,
                streamListener);

        log.info("started consumer = [{}] for keyed stream = [{}]",
                consumerMetadata.getConsumerKey(),
                consumerMetadata.getStreamKey());

        consumerMetadata.setStampingFuture(
                managingService.launchConsumerTimestampUpdatingTask(
                        consumerMetadata.getConsumerKey(), consumerMetadata.getStreamKey()
                )
        );
    }

    private void configureAndStartContainer(String consumerGroup,
                                            ConsumerMetadata consumerMetadata,
                                            RedisConnectionFactory redisConnectionFactory,
                                            StreamListener<String, MapRecord<String, String, String>> streamListener) {
        var options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .build();

        var container = StreamMessageListenerContainer.create(redisConnectionFactory, options);
        var consumer = Consumer.from(consumerGroup, consumerMetadata.getConsumerKey());

        container.receive(consumer, StreamOffset.create(consumerMetadata.getStreamKey(), consumerMetadata.getReadOffset()), streamListener);
        container.start();
    }

    private void scheduleAssignmentWithDelay(RedisStreamConsumerManagingService managingService,
                                             TaskScheduler taskScheduler,
                                             RedisStreamConsumerProperties consumerProperties,
                                             StreamListener<String, MapRecord<String, String, String>> streamListener,
                                             RedisConnectionFactory redisConnectionFactory) {

        var timeoutSeconds = consumerProperties.getTimeoutSeconds();
        var guaranteedExpiration = timeoutSeconds + (timeoutSeconds >> 2);
        var recurringTime = Duration.of(guaranteedExpiration, ChronoUnit.SECONDS);
        var restart = Instant.now().plus(recurringTime);

        log.warn("failed to pair consumer with any stream, launching scheduled process to fix it, " +
                "will attempt again in {}", recurringTime);

        scheduledFuture = taskScheduler.scheduleWithFixedDelay(
                () -> scheduleConsumerAssignment(managingService, consumerProperties, streamListener, redisConnectionFactory, recurringTime),
                restart,
                recurringTime);
    }

    private void scheduleConsumerAssignment(RedisStreamConsumerManagingService managingService,
                                            RedisStreamConsumerProperties consumerProperties,
                                            StreamListener<String, MapRecord<String, String, String>> streamListener,
                                            RedisConnectionFactory redisConnectionFactory,
                                            Duration recurringTime) {
        log.debug("attempting to pair consumer with a stream");

        var metadata = managingService.manageConsumerAssignment();

        if (metadata == null) {
            log.debug("failed to pair a consumer with any existing stream, will attempt again in {}", recurringTime);
            return;
        }

        log.info("success: paired consumer = [{}] to the stream = [{}], terminating scheduled process",
                metadata.getConsumerKey(), metadata.getStreamKey());

        scheduledFuture.cancel(false);
        scheduledFuture = null;

        startListenerContainer(consumerProperties, metadata, managingService, streamListener, redisConnectionFactory);

        replaceBeanDefinition(metadata);
    }

    private void replaceBeanDefinition(ConsumerMetadata metadata) {
        var registry = (BeanDefinitionRegistry) applicationContext.getAutowireCapableBeanFactory();
        var beanDefinition = BeanDefinitionBuilder.rootBeanDefinition(ConsumerMetadata.class)
                .getBeanDefinition();

        beanDefinition.setInstanceSupplier(() -> metadata);
        registry.registerBeanDefinition("consumerMetadata", beanDefinition);
    }
}
