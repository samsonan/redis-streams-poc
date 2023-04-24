package com.samsonan.demo.redisstreams.domain;

import com.samsonan.demo.redisstreams.domain.model.CatalogSuspensionContext;
import com.samsonan.demo.redisstreams.domain.port.out.PublishingCatalogSuspensionPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CatalogSuspensionService {

    private final PublishingCatalogSuspensionPort publishingCatalogSuspensionPort;

    public void processCatalogSuspensionRequest(CatalogSuspensionContext ctx) {
        log.debug("received new request, ctx = [{}]", ctx);

        publishingCatalogSuspensionPort.appendToStream(ctx);
    }

}
