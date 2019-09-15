package com.adobe.platform.core.identity.services.cosmosdb.util;

public class CosmosDbException extends Exception {
    public CosmosDbException(String message, Throwable cause, boolean enableSuppression,
                             boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public CosmosDbException(Throwable cause){
        super(cause);
    }

    public CosmosDbException(String message) {
        super(message, null, true, false);
    }

    public CosmosDbException(String message, Throwable cause) {
        super(message, cause, true, false);
    }
}
