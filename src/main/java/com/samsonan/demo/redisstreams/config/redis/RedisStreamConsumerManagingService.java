package com.samsonan.demo.redisstreams.config.redis;

import com.samsonan.demo.redisstreams.util.NullableTuple;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static com.samsonan.demo.redisstreams.config.app.Constants.CONSUMER_STREAM_KEY;
import static com.samsonan.demo.redisstreams.config.app.Constants.CONSUMER_UPDATED_AT;
import static com.samsonan.demo.redisstreams.config.app.Constants.INITIAL_LOCKING_INTERVAL;
import static com.samsonan.demo.redisstreams.config.app.Constants.LOCKING_ATTEMPTS;
import static com.samsonan.demo.redisstreams.config.app.Constants.LOCK_KEY;
import static com.samsonan.demo.redisstreams.config.app.Constants.MAX_LOCKING_TIME_SECONDS;
import static com.samsonan.demo.redisstreams.config.app.Constants.MAX_LOCK_TRYING_TIME_MILLIS;
import static com.samsonan.demo.redisstreams.config.app.Constants.RETRY_MULTIPLIER;
import static com.samsonan.demo.redisstreams.config.app.Constants.RETRY_RANDOMIZATION_FACTOR;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@Slf4j
@RequiredArgsConstructor
public class RedisStreamConsumerManagingService {

    private final StringRedisTemplate redisTemplate;
    private final RedisStreamConsumerProperties consumerConfig;
    private final TaskScheduler scheduler;

    private String consumerKey;

    private static final RetryConfig RETRY_CONFIG = RetryConfig.<Boolean>custom()
            .maxAttempts(LOCKING_ATTEMPTS)
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                    INITIAL_LOCKING_INTERVAL,
                    RETRY_MULTIPLIER,
                    RETRY_RANDOMIZATION_FACTOR,
                    MAX_LOCK_TRYING_TIME_MILLIS))
            .retryOnResult(locked -> locked == null || !locked)
            .build();

    private static final Retry RETRY = Retry.of("redis_stream_consumer", RETRY_CONFIG);

    static {
        RETRY.getEventPublisher()
                .onRetry(event -> {
                    var retryAttempts = event.getNumberOfRetryAttempts();
                    var timeToNext = event.getWaitInterval();
                    log.warn("failed to acquire global lock, will attempt again in " + timeToNext
                            + ", retries left " + (LOCKING_ATTEMPTS - retryAttempts));
                })
                .onSuccess(event -> log.debug("acquired shared lock, proceeding"));
    }

    /**
     * Find/create consumer metadata for the current instance, so we can assign it to the stream.
     * ConsumerMetadata consists of:
     * - streamKey - stream id
     * - consumerKey - unique consumer id within the stream and consumer group
     * - readOffset - the last not-processed offset
     */
    public ConsumerMetadata manageConsumerAssignment() {
        ConsumerMetadata consumerMetadata = null;
        var tmpId = UUID.randomUUID().toString();

        // we try to acquire a global lock to avoid concurrency conflicts from multiple instances
        // If the lock is not available, we'll try later
        if (!tryLock(tmpId)) {
            log.warn("wasn't able to acquire global lock to make consumer assignments");
            return null;
        }

        try {
            var group = consumerConfig.getGroup();
            var streamKeys = consumerConfig.getStreamKeys();
            var consumerKeys = consumerConfig.getConsumerKeys();
            var consumerStamp = Instant.now();

            createGroupIfNeeded(streamKeys, group);

            consumerMetadata = findFirstUnusedConsumerKey(consumerKeys);

            if (consumerMetadata != null) {
                consumerMetadata.setStreamKey(
                        assignConsumerToStream(consumerKeys, streamKeys, consumerConfig.getConsumerConcurrency())
                );
            } else {
                consumerMetadata = getFailedConsumerMetadata(consumerKeys,
                        consumerConfig.getTimeoutSeconds(),
                        consumerStamp);
            }

            if (consumerMetadata == null) {
                // that could happen if there are more instances than allocated consumer keys - configuration issue
                log.warn("unable to pair stream and consumer");
                return null;
            }

            // find the offset to read from (e.g. last uncommitted from previous consumer)
            consumerKey = consumerMetadata.getConsumerKey();
            var readOffset = ReadOffset.lastConsumed();
            var streamKey = consumerMetadata.getStreamKey();
            var pending = redisTemplate.opsForStream().pending(streamKey, group);
            var unbounded = Range.<String>from(Range.Bound.unbounded()).to(Range.Bound.unbounded());

            if (pending != null && !unbounded.equals(pending.getIdRange())) {
                readOffset = ReadOffset.from(createPreviousOffset(pending.minRecordId()));
            }

            consumerMetadata.setReadOffset(readOffset);
            persistConsumerMetadata(consumerMetadata, consumerStamp, consumerConfig.getTimeoutSeconds());

            return consumerMetadata;
        } catch (Throwable ex) {

            if (consumerMetadata != null) {
                redisTemplate.delete(consumerMetadata.getConsumerKey());
            }

            throw ex;
        } finally {
            redisTemplate.delete(LOCK_KEY);
        }
    }

    private static RecordId createPreviousOffset(RecordId id) {
        var ts = id.getTimestamp();

        if (ts == null) {
            throw new IllegalStateException("offset is bound but timestamp doesn't have any value");
        }

        return RecordId.of(ts - 1, Long.MAX_VALUE);
    }

    public ScheduledFuture<?> launchConsumerTimestampUpdatingTask(String consumerKey, String streamKey) {
        var recurringTime = Duration.of(consumerConfig.getTsUpdateRateSeconds(), ChronoUnit.SECONDS);
        return scheduler.scheduleWithFixedDelay(
                () -> {
                    redisTemplate.opsForHash().put(consumerKey, CONSUMER_STREAM_KEY, streamKey);
                    redisTemplate.opsForHash().put(consumerKey, CONSUMER_UPDATED_AT, Instant.now().toString());
                    redisTemplate.expire(consumerKey, Duration.of(consumerConfig.getTimeoutSeconds(), ChronoUnit.SECONDS));
                },
                Instant.now().plus(recurringTime), recurringTime);
    }

    private boolean tryLock(String stamp) {
        return RETRY.executeSupplier(() -> redisTemplate.opsForValue()
                .setIfAbsent(LOCK_KEY, stamp, Duration.of(MAX_LOCKING_TIME_SECONDS, ChronoUnit.SECONDS)));
    }

    private String assignConsumerToStream(String[] consumerKeys,
                                          String[] streamKeys,
                                          int consumerConcurrency) {

        // get existing consumer metadata from Redis and create map of <stream key> -> [<consumer key>]
        var streamConsumerMap = Arrays.stream(consumerKeys)
                .map(key -> NullableTuple.of(redisTemplate.<String, String>opsForHash().get(key, CONSUMER_STREAM_KEY), key))
                .filter(tuple -> nonNull(tuple.getLeft()))
                .collect(Collectors.toMap(NullableTuple::getLeft,
                        v -> List.of(v.getRight()), (l1, l2) -> {
                            var list = new ArrayList<>(l1);
                            list.addAll(l2);
                            return list;
                        }));

        Arrays.stream(streamKeys)
                .forEach(key -> streamConsumerMap.putIfAbsent(key, new ArrayList<>()));

        // find the empty stream we can assign out consumer and return the stream key
        return streamConsumerMap
                .entrySet()
                .stream()
                .filter(e -> e.getValue().size() < consumerConcurrency)
                .findAny()
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("unable to pair stream and consumers"));
    }

    private void persistConsumerMetadata(ConsumerMetadata consumerMetadata,
                                         Instant consumerStamp,
                                         int expiration) {
        var consumerKey = consumerMetadata.getConsumerKey();
        redisTemplate.opsForHash().put(consumerKey, CONSUMER_UPDATED_AT, consumerStamp.toString());
        redisTemplate.opsForHash().put(consumerKey, CONSUMER_STREAM_KEY, consumerMetadata.getStreamKey());
        redisTemplate.expire(consumerKey, Duration.of(expiration, ChronoUnit.SECONDS));
    }

    private ConsumerMetadata getFailedConsumerMetadata(String[] consumerKeys,
                                                       int timeout,
                                                       Instant consumerStamp) {
        for (var key : consumerKeys) {
            var updatedAt = Instant.parse(
                    requireNonNull(redisTemplate.<String, String>opsForHash().get(key, CONSUMER_UPDATED_AT), "consumer stamp"));

            if (updatedAt.isBefore(consumerStamp.minus(timeout, ChronoUnit.SECONDS))) {

                return ConsumerMetadata.builder()
                        .consumerKey(key)
                        .streamKey(redisTemplate.<String, String>opsForHash().get(key, CONSUMER_STREAM_KEY))
                        .build();
            }
        }

        return null;
    }

    private void createGroupIfNeeded(String[] streamKeys, String group) {
        for (var streamKey : streamKeys) {
            var keyPresent = redisTemplate.hasKey(streamKey);

            if (keyPresent == null || !keyPresent) {
                redisTemplate.opsForStream().createGroup(streamKey, group);
            }
        }
    }

    private ConsumerMetadata findFirstUnusedConsumerKey(String[] consumerKeys) {
        for (var consumerKey : consumerKeys) {
            var keyPresent = redisTemplate.hasKey(consumerKey);

            if (keyPresent == null || !keyPresent) {
                return ConsumerMetadata.builder()
                        .consumerKey(consumerKey)
                        .build();
            }
        }

        return null;
    }

    @PreDestroy
    void tearDown() {
        if (consumerKey != null) {
            redisTemplate.delete(consumerKey);
        }
    }
}
