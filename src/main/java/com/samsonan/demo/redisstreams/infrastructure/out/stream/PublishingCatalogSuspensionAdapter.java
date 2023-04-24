package com.samsonan.demo.redisstreams.infrastructure.out.stream;

import com.samsonan.demo.redisstreams.domain.model.CatalogSuspensionContext;
import com.samsonan.demo.redisstreams.domain.port.out.PublishingCatalogSuspensionPort;
import com.samsonan.demo.redisstreams.infrastructure.InfraUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PublishingCatalogSuspensionAdapter implements PublishingCatalogSuspensionPort {

    private final StringRedisTemplate redisTemplate;
    private final InfraUtil infraUtil;

    @Override
    public void appendToStream(CatalogSuspensionContext ctx) {
        log.debug("appending message: [{}]", ctx);

        var sellerId = ctx.getSellerContractId();
        var record = StreamRecords.objectBacked(ctx)
                .withStreamKey(infraUtil.hashAndPartition(sellerId));

        redisTemplate.opsForStream().add(record);
    }
}
