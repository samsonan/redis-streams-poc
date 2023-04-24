package com.samsonan.demo.redisstreams.infrastructure.in.web.entity.mapper;

import com.samsonan.demo.redisstreams.domain.model.CatalogSuspensionContext;
import com.samsonan.demo.redisstreams.domain.model.RequestOrigin;
import com.samsonan.demo.redisstreams.infrastructure.in.web.entity.CatalogSuspensionEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

import static org.mapstruct.MappingConstants.ComponentModel.SPRING;

@Mapper(componentModel = SPRING, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface CatalogSuspensionDataMapper {

    @Mapping(source = "askedBy", target = "askedBy", qualifiedByName = "strToSuspensionBy")
    CatalogSuspensionContext toDomainObject(CatalogSuspensionEntity catalogSuspensionEntity);

    @Named("strToSuspensionBy")
    static RequestOrigin strToSuspensionBy(String str) {
        return RequestOrigin.convertFromString(str);
    }
}
