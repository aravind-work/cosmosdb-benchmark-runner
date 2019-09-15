package com.adobe.platform.core.identity.services.cosmosdb.util;

public class CosmosDbUtil {
    public static String getDatabaseLink(String dbName){ return "/dbs/" + dbName; }
    public static String getCollectionLink(String dbName, String collectionName){ return "dbs/" + dbName + "/colls/"
            + collectionName; }
    public static String getDocumentLink(String dbName, String collectionName, String docId){ return "dbs/" + dbName
            + "/colls/" + collectionName + "/docs/" + docId; }
}
