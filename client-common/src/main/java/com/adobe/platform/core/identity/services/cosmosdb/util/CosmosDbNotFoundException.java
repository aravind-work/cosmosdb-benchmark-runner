package com.adobe.platform.core.identity.services.cosmosdb.util;

public class CosmosDbNotFoundException extends RuntimeException {
    public CosmosDbNotFoundException(String msg){
        super(msg);
    }
}
