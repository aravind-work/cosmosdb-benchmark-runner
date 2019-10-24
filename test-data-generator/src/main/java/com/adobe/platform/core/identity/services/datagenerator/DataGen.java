package com.adobe.platform.core.identity.services.datagenerator;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.impl.InstagraphDataGen;
import com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen;

public interface DataGen {
    void runDataGeneration();
    void shutdown();

    // Factory method
    static DataGen getInstance(DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        if(datagenConfig.generatorType.equalsIgnoreCase(DataGenType.TWO_TABLE_SIMPLE.toString())){
            return new TwoTableDataGen(datagenConfig, cosmosConfig);
        } else if(datagenConfig.generatorType.equalsIgnoreCase(DataGenType.INSTAGRAPH.toString())){
            return new InstagraphDataGen(datagenConfig, cosmosConfig);
        } else {
            throw new UnsupportedOperationException("Generator type " + datagenConfig.generatorType + " not supported");
        }
    }
}
