package com.adobe.platform.core.identity.services.cosmosdb.util;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;

    static <A> A wrap(ThrowingSupplier<A> checkedFunction) {
        try {
            return checkedFunction.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}