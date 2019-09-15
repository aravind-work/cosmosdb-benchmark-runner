package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * Expecting typesafe config file in the following format
 *
 *  benchmark {
 *      cosmosdb {
 *          serviceEndpoint: "https://va7-dev-identity-cosmosdb-0.documents.azure.com:443/"
 *          dbName: "instagraphv-bm-2"
 *          masterKey: "master-key-sadsd=="
 *          maxPoolSize = 10000
 *          connectionMode = "Direct"
 *          consistencyLevel = "Eventual"
 *          requestTimeoutInMillis = 5000
 *          batchQueryTimeoutInMillis = 5000
 *          batchQueryExecutorThreadPoolSize = 1000
 *      }
 *  }
 *
 */

public class CosmosDbConfig {
    public String serviceEndpoint;
    public String dbName;
    public String masterKey;
    public int maxPoolSize;
    public String connectionMode;
    public String consistencyLevel;
    public int requestTimeoutInMillis;
    public int batchQueryTimeoutInMillis;
    public int batchQueryExecutorThreadPoolSize;

    private static final String CONFIG_PREFIX = "benchmark.cosmosdb.";

    public CosmosDbConfig(){
        this(ConfigFactory.load());
    }

    public CosmosDbConfig(Config config){
        this.serviceEndpoint = config.getString(CONFIG_PREFIX + "serviceEndpoint");
        this.dbName = config.getString(CONFIG_PREFIX + "dbName");
        this.masterKey = config.getString(CONFIG_PREFIX + "masterKey");
        this.maxPoolSize = config.getInt(CONFIG_PREFIX + "maxPoolSize");
        this.connectionMode = config.getString(CONFIG_PREFIX + "connectionMode");
        this.consistencyLevel = config.getString(CONFIG_PREFIX + "consistencyLevel");
        this.requestTimeoutInMillis = config.getInt(CONFIG_PREFIX + "requestTimeoutInMillis");
        this.batchQueryTimeoutInMillis = config.getInt(CONFIG_PREFIX + "batchQueryTimeoutInMillis");
        this.batchQueryExecutorThreadPoolSize = config.getInt(CONFIG_PREFIX + "batchQueryExecutorThreadPoolSize");

        doValidate();
    }

    public String getDatabaseLink(){ return "/dbs/" + this.dbName; }
    public String getDatabaseLink(String dbName){ return "/dbs/" + dbName; }

    public String getCollectionLink(String collectionName){ return "dbs/" + this.dbName + "/colls/" + collectionName; }
    public String getCollectionLink(String dbName, String collectionName){ return "dbs/" + dbName + "/colls/" + collectionName; }

    public String getDocumentLink(String collectionName, String docId){ return "dbs/" + this.dbName + "/colls/"
            + collectionName + "/docs/" + docId; }
    public String getDocumentLink(String dbName, String collectionName, String docId){ return "dbs/" + dbName + "/colls/"
            + collectionName + "/docs/" + docId; }

    private void doValidate(){
        if(this.serviceEndpoint == null ||
                this.serviceEndpoint.length() < 1 ||
                !this.serviceEndpoint.startsWith("http")){
            throw new IllegalArgumentException("Invalid serviceEndpoint " + this.serviceEndpoint + "!");
        }

        if(this.dbName == null || this.dbName.length() < 1 ){
            throw new IllegalArgumentException("Invalid dbName " + this.dbName + "!");
        }

        if(this.masterKey == null || this.masterKey.length() < 1 ){
            throw new IllegalArgumentException("Invalid dbName " + this.dbName + "!");
        }

        if(this.maxPoolSize < -1 || this.maxPoolSize == 0 || this.maxPoolSize > 50000){
            throw new IllegalArgumentException("Invalid maxPoolSize " + this.maxPoolSize + "!");
        }

        if(this.consistencyLevel == null ||
                (   !this.consistencyLevel.equalsIgnoreCase("eventual") &&
                    !this.consistencyLevel.equalsIgnoreCase("boundedstaleness") &&
                    !this.consistencyLevel.equalsIgnoreCase("strong"))  ) {
            throw new IllegalArgumentException("Invalid consistencyLevel " + this.consistencyLevel + "!");
        }

        if(this.connectionMode == null ||
                (   !this.connectionMode.equalsIgnoreCase("direct") &&
                    !this.connectionMode.equalsIgnoreCase("gateway"))   ) {
            throw new IllegalArgumentException("Invalid connectionMode " + this.connectionMode + "!");
        }
    }

}
