package com.samsonan.demo.redisstreams.util;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class NullableTuple<K, V> {

    private K left;
    private V right;

    public static <K, V> NullableTuple<K, V> of(K left, V right) {
        return new NullableTuple<>(left, right);
    }
}
