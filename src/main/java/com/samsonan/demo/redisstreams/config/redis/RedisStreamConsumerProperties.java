package com.samsonan.demo.redisstreams.config.redis;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RedisStreamConsumerProperties {
    private String group;
    private String[] streamKeys;
    private String[] consumerKeys;
    private int consumerConcurrency;
    private int timeoutSeconds;
    private int tsUpdateRateSeconds;
}
