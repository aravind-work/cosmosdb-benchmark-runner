package com.adobe.platform.core.identity.services.datagenerator;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen;

public interface DataGen {
    void runDataGeneration();
    void shutdown();

    // Factory method
    static DataGen getInstance(DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        if(datagenConfig.generatorType.equalsIgnoreCase(DataGenType.TWO_TABLE_SIMPLE.toString().toLowerCase())){
            return new TwoTableDataGen(datagenConfig, cosmosConfig);
        } else {
            throw new UnsupportedOperationException("Generator type " + datagenConfig.generatorType + " not supported");
        }
    }
}
