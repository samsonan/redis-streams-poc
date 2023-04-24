package com.samsonan.demo.redisstreams.infrastructure.in.stream;

import com.samsonan.demo.redisstreams.config.redis.RedisStreamConsumerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@RequiredArgsConstructor
public class CatalogSuspensionStreamConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private final StringRedisTemplate redisTemplate;
    private final RedisStreamConsumerProperties consumerProperties;

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        try {
            log.debug("received message = [{}], id = [{}]", message.getValue(), message.getId());

        } catch (Exception ex) {
            log.error("encountered an exception during processing of a stream message", ex);
        } finally {
            log.debug("committing offset for message id = [{}]", message.getId());

            redisTemplate.opsForStream().acknowledge(consumerProperties.getGroup(), message);
        }
    }
}
