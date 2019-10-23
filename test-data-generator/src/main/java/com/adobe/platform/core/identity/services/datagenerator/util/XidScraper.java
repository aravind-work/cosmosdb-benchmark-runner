package com.adobe.platform.core.identity.services.datagenerator.util;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tool to fetch ids across various partitions and write to a text file
 */
public class XidScraper {
    private static final Logger logger = LoggerFactory.getLogger(XidScraper.class.getSimpleName());

    private static final String ROUTING_COLLECTION_NAME = "routing";
    private static final int MAX_IDS_TO_FETCH_PER_PARTITION = 1000;

    private static final String XID_FILE_NAME = "/tmp/xids2.txt";
    public static void main(String args[]) throws IOException {

        CosmosDbConfig cosmosConfig = new CosmosDbConfig();
        DataGenConfig datagenConfig = new DataGenConfig();

        String routingCollectionName = datagenConfig.collectionPrefix + ROUTING_COLLECTION_NAME;

        AsyncCosmosDbClient asyncClient = new AsyncCosmosDbClient(cosmosConfig);

        asyncClient.verifyCollectionsExist(Arrays.asList(routingCollectionName));

        // Get keys from each partition for querying
        List<List<String>> idsPerPartition = asyncClient
                .getIdsPerPartition(routingCollectionName, MAX_IDS_TO_FETCH_PER_PARTITION)
                .toBlocking().single();

        int partitionCount = idsPerPartition.size();
        long docCount = idsPerPartition.stream().flatMap(List::stream).count();
        logger.info("Fetched {} ids from {} partitions.", docCount, partitionCount);

        Path path = Paths.get(XID_FILE_NAME);
        logger.info("Writing ids to {}", path);
        String combinedIds = idsPerPartition.stream().flatMap(List::stream).collect(Collectors.joining("\n"));

        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            try {
                writer.write(combinedIds);
            } catch (IOException e){
                logger.error("Unable to write XID list {}", path.toString());
            }
        }
        logger.info("Finished");
    }
}
