package com.samsonan.demo.redisstreams.infrastructure.in.web.entity;

import com.samsonan.demo.redisstreams.domain.model.ProductActivityState;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CatalogSuspensionEntity {

    @NotNull
    @Pattern(regexp = "^(platform|seller)$",
            message = "value's not allowed, should be either \"platform\" or \"seller\"")
    private String askedBy;

    private long sellerContractId;
    private ProductActivityState productActivityState;
}
