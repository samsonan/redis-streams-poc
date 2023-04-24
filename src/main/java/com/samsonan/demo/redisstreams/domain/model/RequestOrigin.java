package com.samsonan.demo.redisstreams.domain.model;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum RequestOrigin {
    PLATFORM("platform"), SELLER("seller");

    private static final Map<String, RequestOrigin> serializedToEnum = Arrays.stream(RequestOrigin.values())
            .collect(Collectors.toMap(k -> k.serializedValue, Function.identity()));

    private final String serializedValue;

    RequestOrigin(String serializedValue) {
        this.serializedValue = serializedValue;
    }

    public static RequestOrigin convertFromString(String val) {
        return serializedToEnum.get(val);
    }
}
