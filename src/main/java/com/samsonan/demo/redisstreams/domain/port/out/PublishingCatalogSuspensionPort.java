package com.samsonan.demo.redisstreams.domain.port.out;

import com.samsonan.demo.redisstreams.domain.model.CatalogSuspensionContext;

public interface PublishingCatalogSuspensionPort {

    void appendToStream(CatalogSuspensionContext catalogSuspensionProcess);
}
