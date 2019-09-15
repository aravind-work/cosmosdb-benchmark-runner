package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;

import java.util.List;

public interface CosmosDbClient {
    SimpleDocument readDocument(String collectionName, String docId) throws CosmosDbException;
    SimpleDocument createDocument(String collectionName, SimpleDocument sDoc) throws CosmosDbException;

    List<SimpleDocument> readDocuments(String collectionName, List<String> docIdList, int batchQueryMaxSize) throws CosmosDbException;
    CosmosDbConfig getConfig();
    void close();
}
