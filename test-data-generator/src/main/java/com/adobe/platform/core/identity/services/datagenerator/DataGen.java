package com.adobe.platform.core.identity.services.datagenerator;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen;

public interface DataGen {
    // used during data generation
    boolean rebuildCollections();
    void generateTestData();
    boolean validateTestData();

    // used during benchmark
    void verifyCollections();
    void fetchQueryKeys();

    String TWO_TABLE_SIMPLE_GENERATOR_TYPE = "two-table-simple";

    static DataGen getInstance(AsyncCosmosDbClient client, DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        if(datagenConfig.generatorType.equalsIgnoreCase(TWO_TABLE_SIMPLE_GENERATOR_TYPE)){
            return new TwoTableDataGen(client, datagenConfig, cosmosConfig);
        } else {
            throw new UnsupportedOperationException("Generator type " + datagenConfig.generatorType + " not supported");
        }
    }
}
