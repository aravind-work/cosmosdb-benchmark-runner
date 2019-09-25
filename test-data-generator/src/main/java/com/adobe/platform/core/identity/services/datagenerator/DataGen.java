package com.adobe.platform.core.identity.services.datagenerator;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleResponse;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;
import com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen;
import com.microsoft.azure.cosmosdb.Document;

import java.util.List;
import java.util.Map;

public interface DataGen {
    void workloadSetup();
    void runDataGeneration();

    ThrowingSupplier<SimpleResponse> lookupRoutingSingle();
    ThrowingSupplier<SimpleResponse> lookupRoutingBatch();
    ThrowingSupplier<SimpleResponse> lookupTwoTableSingle();
    ThrowingSupplier<SimpleResponse> lookupTwoTableBatch();

    // Factory method
    static DataGen getInstance(AsyncCosmosDbClient client, DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        if(datagenConfig.generatorType.equalsIgnoreCase(DataGenType.TWO_TABLE_SIMPLE.toString().toLowerCase())){
            return new TwoTableDataGen(client, datagenConfig, cosmosConfig);
        } else {
            throw new UnsupportedOperationException("Generator type " + datagenConfig.generatorType + " not supported");
        }
    }
}
