package com.adobe.platform.core.identity.services.datagenerator.main;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.DataGen;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenUtil {

    private static final Logger logger = LoggerFactory.getLogger(DataGenUtil.class.getSimpleName());

    public static void main(String[] args){
        // Create config objects
        CosmosDbConfig cosmosConfig = new CosmosDbConfig();
        DataGenConfig dataGenConfig = new DataGenConfig();

        // Create cosmos client
        AsyncCosmosDbClient client = new AsyncCosmosDbClient(cosmosConfig);

        // Run Data Generator
        DataGen generator = DataGen.getInstance(client, dataGenConfig, cosmosConfig);

        run(generator, dataGenConfig, client);

        // Close client
        client.close();

        logger.info("Data Generator has finished.");
        System.exit(0);
    }

    public static void run(DataGen generator, DataGenConfig cfg, AsyncCosmosDbClient client){

        // Rebuild tables and populate with test data
        if (cfg.reCreateCollections) {
            logger.info("Re-creating tables ...");

            if(generator.rebuildCollections()){
                logger.info("Re-creating tables complete.");
            } else {
                throw new RuntimeException("recreateCosmosDBTables failed!");
            }
        } else {
            logger.info("Re-creating tables ... [SKIPPED]");
        }

        if (cfg.generateTestData) {
            generator.generateTestData();
            logger.info("Data generation complete.");
        } else {
            logger.info("Generating test data ... [SKIPPED]");
        }

        // Assert test collections have right number of documents
        if(cfg.performDocCountCheck) {
            logger.info("Data validation in-progress ...");
            generator.validateTestData();
            logger.info("Data validation complete.");
        }

    }
}