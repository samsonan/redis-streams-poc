package com.samsonan.demo.redisstreams.config.redis;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.redis.connection.stream.ReadOffset;

import java.util.concurrent.ScheduledFuture;

@Getter
@Setter
@Builder
public class ConsumerMetadata {
    private String streamKey;
    private String consumerKey;
    private ReadOffset readOffset;
    private ScheduledFuture<?> stampingFuture;
}
