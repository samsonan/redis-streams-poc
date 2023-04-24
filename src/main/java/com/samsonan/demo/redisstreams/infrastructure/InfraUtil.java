package com.samsonan.demo.redisstreams.infrastructure;

import com.samsonan.demo.redisstreams.config.redis.RedisStreamConsumerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InfraUtil {

    private final RedisStreamConsumerProperties consumerConfig;

    public String hashAndPartition(long eventKey) {
        var streamKeys = consumerConfig.getStreamKeys();
        return streamKeys[(int) eventKey % streamKeys.length];
    }
}
