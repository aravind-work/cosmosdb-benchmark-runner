package com.adobe.platform.core.identity.services.datagenerator.impl;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleDocument;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;
import com.adobe.platform.core.identity.services.datagenerator.AbstractDataGen;
import com.adobe.platform.core.identity.services.datagenerator.DataGen;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TwoTableDataGen extends AbstractDataGen implements DataGen {
    Logger logger = LoggerFactory.getLogger(TwoTableDataGen.class.getSimpleName());

    static final String ROUTING_COLL_PROTO_FIELD = "rp";
    static final String GRAPH_COLL_PROTO_FIELD = "gp";

    static final int BATCH_QUERY_SIZE = 1000;
    static final int MAX_PER_PARTITION_QUERY_BATCH_SIZE = 10;

    // instance level
    private final String routingCollectionName;
    private final String graphCollectionName;

    private List<List<String>> idsPerPartition;


    public TwoTableDataGen(AsyncCosmosDbClient client, DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        super(client, datagenConfig, cosmosConfig);
        routingCollectionName = datagenConfig.tablePrefix + "routing";
        graphCollectionName = datagenConfig.tablePrefix + "graph";
    }

    // workload-1
    public ThrowingSupplier<String> lookupRoutingSingle(){
        return () -> {

            String queryXid = selectRandomDocId(idsPerPartition);
            SimpleDocument doc = client.readDocument(routingCollectionName, queryXid);
            String graphId = (String) doc.properties.get(ROUTING_COLL_PROTO_FIELD);

            assert (graphId.length() > 16);
            return graphId;
        };
    }

    // workload-2
    public ThrowingSupplier<List<String>> lookupRoutingBatch() {
        return () -> {

            List<String> queryXids = new ArrayList<>(selectDocIdsAcrossPartitions(BATCH_QUERY_SIZE, idsPerPartition));
            List<SimpleDocument> docs = client.readDocuments(routingCollectionName, queryXids,
                    MAX_PER_PARTITION_QUERY_BATCH_SIZE);
            return docs.stream().map(d -> {
                String graphId = (String) d.properties.get(ROUTING_COLL_PROTO_FIELD);
                assert (graphId.length() > 16);
                return graphId;
            }).collect(Collectors.toList());
        };
    }

    // workload-3
    public ThrowingSupplier<String> lookupTwoTableSingle() {
        return () -> {

            String graphId = lookupRoutingSingle().get();
            SimpleDocument doc = client.readDocument(graphCollectionName, graphId);
            String graphDoc = (String) doc.properties.get(ROUTING_COLL_PROTO_FIELD);

            assert (graphDoc.length() > 1);
            return graphDoc;
        };
    }

    // workload-4
    public ThrowingSupplier<List<String>> lookupTwoTableBatch() {
        return () -> {

            List<String> graphIds = lookupRoutingBatch().get();
            List<SimpleDocument> docs = client.readDocuments(graphCollectionName, graphIds,
                                                                MAX_PER_PARTITION_QUERY_BATCH_SIZE);

            assert (docs.size() == BATCH_QUERY_SIZE);
            return docs.stream().map(doc -> {
                String graphDoc = (String) doc.properties.get(ROUTING_COLL_PROTO_FIELD);
                assert (graphDoc.length() > 1);
                return graphDoc;
            }).collect(Collectors.toList());
        };
    }


    @Override
    public synchronized boolean rebuildCollections() {
        return recreateCollections(Arrays.asList(routingCollectionName, graphCollectionName));
    }

    @Override
    public synchronized void generateTestData() {
        logger.info("Generating test data in-process ...");
        List<Pair<String, List<String>>> testData = Stream.generate(() -> UUID.randomUUID().toString())
            .limit(datagenConfig.testGraphCount)
            .map(graphId -> {
                List<String> xids = Stream.generate(() -> UUID.randomUUID().toString())
                        .limit(datagenConfig.nodesPerGraph).collect(Collectors.toList());
                Pair<String, List<String>> tmp = Pair.of(graphId, xids);
                return tmp; })
            .collect(Collectors.toList());
        logger.info("Generating test data in-process complete.");

        // Write routing table entries
        Observable<Integer> routingSuccessCountObs = Observable.from(testData)
            .flatMapIterable(entry -> {
                String graphId = entry.getKey();
                List<String> xids = entry.getValue();
                return xids.stream().map(xid -> Pair.of(xid, graphId)).collect(Collectors.toList()); })
            .flatMap(entry -> {
                //write routing
                String xid = entry.getLeft();
                String graphId = entry.getRight();

                Document doc = new Document();
                doc.setId(xid);
                doc.set(ROUTING_COLL_PROTO_FIELD, graphId);

                return client.createDocument(routingCollectionName, doc); })
            .retry()
            .filter(a -> a.getStatusCode() >= 200 && a.getStatusCode() < 300)
            .count();

        // Wait for routing table to be populated
        logger.info("Writing test data to Routing table started ...");
        int routingSuccessCount = routingSuccessCountObs
                .subscribeOn(Schedulers.computation())
                .toBlocking().single();
        if(routingSuccessCount != datagenConfig.testGraphCount * datagenConfig.nodesPerGraph){
            throw new RuntimeException("Expected to receive " + datagenConfig.testGraphCount * datagenConfig.nodesPerGraph +
                    " 200 OK status responses but got only " + routingSuccessCount +" !");
        }
        logger.info("Writing test data to Routing table complete.");


        // Write graph table entries
        Observable<Integer> graphSuccessCountObs =  Observable.from(testData)
            .flatMap(entry -> {
                String graphId = entry.getKey();
                List<String> xids = entry.getValue();
                String xidsCsv = String.join(",", xids);

                Document doc = new Document();
                doc.setId(graphId);
                doc.set(GRAPH_COLL_PROTO_FIELD, xidsCsv);
                RequestOptions options =  new RequestOptions();
                options.setPartitionKey(new PartitionKey(graphId));

                return client.createDocument(datagenConfig.tablePrefix + graphCollectionName, doc); })
            .filter(a -> a.getStatusCode() >= 200 && a.getStatusCode() < 300)
            .count();

        // Wait for graph table to be populated
        logger.info("Writing test data to Graph table started ...");
        int graphSuccessCount = graphSuccessCountObs
                .subscribeOn(Schedulers.computation())
                .toBlocking().single();
        if(graphSuccessCount != datagenConfig.testGraphCount){
            throw new RuntimeException("Expected to receive " + datagenConfig.testGraphCount + " 200 OK status responses but " +
                    "                   got only " + routingSuccessCount +" !");
        }
        logger.info("Writing test data to Graph table complete.");
        logger.info("Generating test data complete.");
    }

    @Override
    public synchronized boolean validateTestData() {
        // Assert test collections have right number of documents
        logger.info("Querying for document count of routing table ...");

        //todo: The number from this query doesn't match what's in cosmos. This check is currently not useful.
        long routingCollectionDocumentCount = client
                .getCollectionSize(routingCollectionName)
                .subscribeOn(Schedulers.computation())
                .toBlocking().single();

        long graphCollectionDocumentCount = client
                .getCollectionSize(graphCollectionName)
                .subscribeOn(Schedulers.computation())
                .toBlocking().single();
        logger.info("Querying for document count of routing table complete.");

        if (graphCollectionDocumentCount != datagenConfig.testGraphCount ||
                routingCollectionDocumentCount != datagenConfig.testGraphCount * datagenConfig.nodesPerGraph) {
            throw new RuntimeException(String.format("Expected graphCollectionDocumentCount to be %d but found %d. "
                            + "Expected routingCollectionDocumentCount to be %d but found %d",
                    datagenConfig.testGraphCount, graphCollectionDocumentCount,
                    datagenConfig.testGraphCount * datagenConfig.nodesPerGraph, routingCollectionDocumentCount));
        } else {
            logger.info("Document counts match expected number.");
            return true;
        }
    }


    @Override
    public synchronized void verifyCollections(){
        verifyCollectionsExist(Arrays.asList(routingCollectionName, graphCollectionName));
    }

    public synchronized void fetchQueryKeys(){
        this.idsPerPartition = fetchQueryKeys(routingCollectionName);
    }

}
