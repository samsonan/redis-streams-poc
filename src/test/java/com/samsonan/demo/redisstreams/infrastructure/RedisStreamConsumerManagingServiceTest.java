package com.samsonan.demo.redisstreams.infrastructure;

import com.samsonan.demo.redisstreams.BaseIntegrationTest;
import com.samsonan.demo.redisstreams.config.redis.ConsumerMetadata;
import com.samsonan.demo.redisstreams.config.redis.RedisStreamConsumerManagingService;
import com.samsonan.demo.redisstreams.config.redis.RedisStreamConsumerProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.samsonan.demo.redisstreams.config.app.Constants.CONSUMER_STREAM_KEY;
import static com.samsonan.demo.redisstreams.config.app.Constants.CONSUMER_UPDATED_AT;
import static com.samsonan.demo.redisstreams.config.app.Constants.LOCKING_ATTEMPTS;
import static com.samsonan.demo.redisstreams.config.app.Constants.LOCK_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(OutputCaptureExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class RedisStreamConsumerManagingServiceTest extends BaseIntegrationTest {

    @Autowired
    private ConsumerMetadata consumerMetadata;

    @Autowired
    private RedisStreamConsumerProperties consumerConfig;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RedisStreamConsumerManagingService managingService;

    @Test
    @DisplayName("stream keys should be created")
    public void creationOfMissingStreamGroups() {
        for (var key : consumerConfig.getStreamKeys()) {
            assertEquals(Boolean.TRUE, redisTemplate.hasKey(key));
        }
    }

    @Test
    @DisplayName("consumer should be assigned to a stream")
    public void consumerInitializationTest() {
        assertNotNull(consumerMetadata.getConsumerKey());
        assertNotNull(consumerMetadata.getStreamKey());
        assertNotNull(consumerMetadata.getReadOffset());
        assertNotNull(consumerMetadata.getStampingFuture());

        assertTrue(Arrays.stream(consumerConfig.getConsumerKeys())
                .anyMatch(key -> key.equals(consumerMetadata.getConsumerKey())));
        assertTrue(Arrays.stream(consumerConfig.getStreamKeys())
                .anyMatch(key -> key.equals(consumerMetadata.getStreamKey())));
        assertFalse(consumerMetadata.getStampingFuture().isDone());
        assertFalse(consumerMetadata.getStampingFuture().isCancelled());

        assertNotNull(redisTemplate.opsForHash().get(consumerMetadata.getConsumerKey(), CONSUMER_STREAM_KEY));
        assertNotNull(redisTemplate.opsForHash().get(consumerMetadata.getConsumerKey(), CONSUMER_UPDATED_AT));
    }

    @Test
    @DisplayName("assignment shouldn't depend on any ordering")
    public void outOfOrderAssignment() {

        for (var key : consumerConfig.getConsumerKeys()) {
            redisTemplate.delete(key);
        }

        var consumers = consumerConfig.getConsumerKeys();
        var streams = consumerConfig.getStreamKeys();
        var consumerConcurrency = consumerConfig.getConsumerConcurrency();

        for (int i = 0; i < streams.length - 1; i++) {
            int j = consumers.length - i * consumerConcurrency - 1;
            for (int k = 0; k < consumerConcurrency; k++) {
                redisTemplate.opsForHash().put(consumers[j], CONSUMER_STREAM_KEY, streams[i]);
            }
        }

        managingService.manageConsumerAssignment();

        assertEquals(streams[0], redisTemplate.opsForHash().get(consumers[consumers.length - 1], CONSUMER_STREAM_KEY));
    }

    @Test
    @DisplayName("replace timed out consumer")
    public void replacingTimedOutConsumer() {
        var cancelled = consumerMetadata.getStampingFuture().cancel(true);
        assertTrue(cancelled);

        for (var key : consumerConfig.getConsumerKeys()) {
            redisTemplate.delete(key);
        }

        var consumers = consumerConfig.getConsumerKeys();

        IntStream.range(0, consumers.length)
                .forEach(unused -> managingService.manageConsumerAssignment());

        var lateDate = Instant.now().minus(1, ChronoUnit.DAYS);

        redisTemplate.opsForHash().put(consumers[0], CONSUMER_UPDATED_AT, lateDate.toString());

        managingService.manageConsumerAssignment();

        var strInstant = redisTemplate.<String, String>opsForHash().get(consumers[0], CONSUMER_UPDATED_AT);
        assertNotNull(strInstant);
        var replacesStamp = Instant.parse(strInstant);

        assertTrue(replacesStamp.isAfter(Instant.now().minus(1, ChronoUnit.HOURS)));
    }

    @Test
    @DisplayName("lock acquisition failure logged")
    public void testUnableToAcquireLock(CapturedOutput capturedOutput) {
        redisTemplate.opsForValue().set(LOCK_KEY, UUID.randomUUID().toString());

        managingService.manageConsumerAssignment();

        assertTrue(capturedOutput.getAll().contains("will attempt again in"));
        assertTrue(capturedOutput.getAll().contains("retries left " + (LOCKING_ATTEMPTS - 1)));
        assertTrue(capturedOutput.getAll().contains("retries left 1"));
    }
}
