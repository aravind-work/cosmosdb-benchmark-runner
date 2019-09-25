package com.adobe.platform.core.identity.services.datagenerator;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import javax.print.Doc;
import java.util.*;

public abstract class AbstractDataGen implements DataGen {
    Logger logger = LoggerFactory.getLogger(AbstractDataGen.class.getSimpleName());

    private static final int MAX_IDS_TO_FETCH_PER_PARTITION = 1000;
    private static final Random rnd = new Random();

    // instance level
    protected final AsyncCosmosDbClient client;
    protected final DataGenConfig datagenConfig;
    protected final CosmosDbConfig cosmosConfig;

    public AbstractDataGen(AsyncCosmosDbClient client, DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        this.client = client;
        this.datagenConfig = datagenConfig;
        this.cosmosConfig = cosmosConfig;
    }

    protected abstract List<String> getRequiredCollectionList();
    protected abstract Map<String, List<Document>> generateTestData();
    protected abstract boolean validateTestData();

    // Entry point to perform collection creation and test-data generation
    public synchronized void runDataGeneration(){
        // Rebuild collections and populate with test data
        if (datagenConfig.reCreateCollections) {
            if(!recreateCollections(getRequiredCollectionList())){
                throw new RuntimeException("recreateCollections failed!");
            }
        } else {
            logger.info("Re-creating collections ... [SKIPPED]");
        }

        if (datagenConfig.generateTestData) {
            writeTestDataToCollections(generateTestData());
        } else {
            logger.info("Generating test data ... [SKIPPED]");
        }

        // Assert test collections have right number of documents
        if(datagenConfig.performTestDataValidation) {
            logger.info("Data validation in-progress ...");
            validateTestData();
            logger.info("Data validation complete.");
        }
    }

    private synchronized void writeTestDataToCollections(Map<String, List<Document>> docs){
        long docCount = docs.values().stream().map(a -> a.size()).mapToLong(Long::valueOf).sum();
        logger.info("Writing {} test documents to collections ...", docCount);

        List<ResourceResponse<Document>> results = Observable.from(docs.entrySet())
                .flatMap(entry -> Observable.from(entry.getValue()).map(a -> Pair.of(entry.getKey(), a)))
                .flatMap(pair -> client.createDocument(pair.getKey(), pair.getValue()))
                .toList()
                .subscribeOn(Schedulers.computation())
                .toBlocking()
                .single();

        long successCount = results.stream().map(ResourceResponse::getStatusCode).filter(t -> t >= 200 && t < 300).count();
        if(successCount != docCount){
            throw new RuntimeException("Expected to receive " + docCount + " 200 OK status responses but got only "
                    + successCount +" !"); }

        logger.info("Writing test data complete.");
    }

    protected synchronized List<List<String>> fetchQueryKeys(String collectionName){
        Logger logger = LoggerFactory.getLogger(AbstractDataGen.class.getSimpleName());

        // Fetch test data from DB
        logger.info("Fetching ids from each partition to use in queries ...");

        List<List<String>> idsPerPartition =
                client.getIdsPerPartition(collectionName, MAX_IDS_TO_FETCH_PER_PARTITION)
                        .toBlocking().single();

        int partitionCount = idsPerPartition.size();
        long docCount = idsPerPartition.stream().flatMap(List::stream).count();

        logger.info("Fetched {} ids from {} partitions.", docCount, partitionCount);
        return idsPerPartition;
    }


    private synchronized boolean recreateCollections(List<String> collectionNames) {
        logger.info("Recreating Collections {} ...", String.join(",", collectionNames));

        return Observable.from(collectionNames)
                .flatMap(c -> Observable.concat(
                    client.deleteCollection(datagenConfig.collectionPrefix + c),
                    client.createCollection(datagenConfig.collectionPrefix + c, DataGenConfig.P_KEY, datagenConfig.collectionRusOnCreate,
                                        datagenConfig.collectionRusPostCreate, datagenConfig.createWithConsistencyLevel)))
                .reduce((a,b) -> a && b)
                .subscribeOn(Schedulers.computation())
                .doOnCompleted(() -> logger.info("Recreating Collections {} complete.", String.join(",", collectionNames)))
                .toBlocking().single();
    }

    protected synchronized void verifyCollectionsExist(List<String> collectionNames){
        logger.info("Verifying that Database/Collections exists ...");
        client.getDatabase()
                .flatMap(db -> {
                    logger.info("Database exists.");
                    return Observable.from(collectionNames)
                            .flatMap(c -> client.getCollection(cosmosConfig.dbName, datagenConfig.collectionPrefix + c))
                            .toList(); })
                .subscribeOn(Schedulers.computation())
                .doOnCompleted(() -> logger.info("Verified that collections exists."))
                .toBlocking().single();
    }

    // Helper functions

    public static int selectRandomPartitionRangeId(List<List<String>> idsPerPartition){
        return rnd.nextInt(idsPerPartition.size());
    }

    public static String selectRandomDocId(List<List<String>> idsPerPartition){
        int partitionId = selectRandomPartitionRangeId(idsPerPartition);
        return selectRandomDocId(partitionId, idsPerPartition);
    }

    public static String selectRandomDocId(int partitionId, List<List<String>> idsPerPartition){
        List<String> keysInThisPartition = idsPerPartition.get(partitionId);
        int index = rnd.nextInt(keysInThisPartition.size());
        return keysInThisPartition.get(index);
    }

    public static Set<String> selectDocIdsAcrossPartitions(int keyCount, List<List<String>> idsPerPartition){
        int docCount = idsPerPartition.stream().mapToInt(List::size).sum();
        if(keyCount > docCount){
            throw new RuntimeException("Number of documents requested documents (" + keyCount + ") is > total documents " +
                    "fetched for benchmark queries (" + docCount + ").");
        }
        Set<String> keys = new HashSet<>(keyCount);

        int currPartition = 0;
        int currPos = 0;

        while(keys.size() < keyCount){
            keys.add(idsPerPartition.get(currPartition).get(currPos));  // pick a key in this partition

            // loop through the partitions
            currPartition++;
            if(currPartition >= idsPerPartition.size()){
                currPartition = 0;  // start from the beginning
                currPos++; // get the element at currPos from each partition
            }
        }

        return keys;
    }
}
