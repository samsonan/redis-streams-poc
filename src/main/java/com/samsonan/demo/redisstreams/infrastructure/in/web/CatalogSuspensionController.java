package com.samsonan.demo.redisstreams.infrastructure.in.web;

import com.samsonan.demo.redisstreams.domain.CatalogSuspensionService;
import com.samsonan.demo.redisstreams.domain.model.CatalogSuspensionContext;
import com.samsonan.demo.redisstreams.domain.model.ProductActivityState;
import com.samsonan.demo.redisstreams.infrastructure.in.web.entity.CatalogSuspensionEntity;
import com.samsonan.demo.redisstreams.infrastructure.in.web.entity.mapper.CatalogSuspensionDataMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@ResponseStatus(HttpStatus.NO_CONTENT)
@RequestMapping("/seller-management/monechelle/api/v0/sellers/{sellerContractId}/catalog")
public class CatalogSuspensionController {

    private final CatalogSuspensionDataMapper dataMapper;
    private final CatalogSuspensionService suspensionService;

    @PutMapping("/suspend")
    public void suspend(@NotNull
                        @PathVariable
                        @Pattern(regexp = "^\\d+$", message = "seller id contains only digits")
                        String sellerContractId,
                        @Valid
                        @RequestBody
                        CatalogSuspensionEntity catalogSuspensionEntity) {
        var data = enrichAndMapToDomainObject(sellerContractId, catalogSuspensionEntity, ProductActivityState.SUSPENSION);
        suspensionService.processCatalogSuspensionRequest(data);
    }

    private CatalogSuspensionContext enrichAndMapToDomainObject(String sellerContractId,
                                                                CatalogSuspensionEntity catalogSuspensionEntity,
                                                                ProductActivityState productActivityState) {
        catalogSuspensionEntity.setSellerContractId(Long.parseLong(sellerContractId));
        catalogSuspensionEntity.setProductActivityState(productActivityState);
        return dataMapper.toDomainObject(catalogSuspensionEntity);
    }
}
