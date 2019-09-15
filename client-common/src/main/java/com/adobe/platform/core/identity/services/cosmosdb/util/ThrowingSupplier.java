package com.adobe.platform.core.identity.services.cosmosdb.util;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}