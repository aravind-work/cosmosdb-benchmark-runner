package com.adobe.platform.core.identity.services.datagenerator.main;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.DataGen;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenUtil {

    private static final Logger logger = LoggerFactory.getLogger(DataGenUtil.class.getSimpleName());

    public static void main(String[] args) {
        // Create config objects
        CosmosDbConfig cosmosConfig = new CosmosDbConfig();
        DataGenConfig dataGenConfig = new DataGenConfig();

        // Create cosmos client
        AsyncCosmosDbClient client = new AsyncCosmosDbClient(cosmosConfig);

        // Run Data Generator
        DataGen generator = DataGen.getInstance(client, dataGenConfig, cosmosConfig);
        generator.runDataGeneration();

        // Close client
        client.close();

        logger.info("Data Generator has finished.");
        System.exit(0);
    }
}