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

    protected final AsyncCosmosDbClient client;
    protected final DataGenConfig datagenConfig;
    protected final CosmosDbConfig cosmosConfig;

    public AbstractDataGen(DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        this.client = new AsyncCosmosDbClient(cosmosConfig);
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


}
