package com.adobe.platform.core.identity.services.cosmosdb.client;

public class CosmosConstants {
    static final String COUNT_QUERY = "SELECT VALUE COUNT(1) FROM c";
    static final String ROOT_QUERY = "SELECT * FROM root r WHERE r.id=@id";
    static final String OFFER_QUERY_STR_FMT = "SELECT * FROM r WHERE r.offerResourceId = '%s'";
    private static final String QUERY_STRING = "SELECT * FROM root r WHERE r.%s=@id";
    static final String QUERY_STRING_BATCH = "SELECT * FROM root r WHERE r.%s IN (%s)";
    static final String AGGREGATE_PROPERTY = "_aggregate";
    static final String QUERY_NAMED_PARAM_PREFIX = "@tempParam_";
    static final String COSMOS_DEFAULT_COLUMN_KEY = "id";
    private static final int MAX_ITEM_COUNT = -1;
}
