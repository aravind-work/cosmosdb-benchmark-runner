package com.adobe.platform.core.identity.services.cosmosdb.util;

import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T,R> {
    R apply(T t) throws Exception;

    static <A,B> Function<A,B> wrap(ThrowingFunction<A,B> checkedFunction) {
        return t -> {
            try {
                return checkedFunction.apply(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}

