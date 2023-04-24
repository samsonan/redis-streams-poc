package com.samsonan.demo.redisstreams.domain.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class CatalogSuspensionContext {
    private RequestOrigin askedBy;
    private long sellerContractId;
    private ProductActivityState productActivityState;
}
