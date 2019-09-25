package com.adobe.platform.core.identity.services.datagenerator.impl;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleDocument;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleResponse;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;
import com.adobe.platform.core.identity.services.datagenerator.AbstractDataGen;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import com.microsoft.azure.cosmosdb.Document;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TwoTableDataGen extends AbstractDataGen {
    Logger logger = LoggerFactory.getLogger(TwoTableDataGen.class.getSimpleName());

    static final String ROUTING_COLL_PROTO_FIELD = "rp";
    static final String GRAPH_COLL_PROTO_FIELD = "gp";

    static final int BATCH_QUERY_SIZE = 1000;
    static final int MAX_PER_PARTITION_QUERY_BATCH_SIZE = 100;

    // instance level
    private final String routingCollectionName;
    private final String graphCollectionName;

    private List<List<String>> idsPerPartition;


    public TwoTableDataGen(AsyncCosmosDbClient client, DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        super(client, datagenConfig, cosmosConfig);
        routingCollectionName = datagenConfig.collectionPrefix + "routing";
        graphCollectionName = datagenConfig.collectionPrefix + "graph";
    }

    // ---------- Interface methods ----------

    @Override
    public synchronized void workloadSetup(){
        verifyCollectionsExist(getRequiredCollectionList());
        this.idsPerPartition = fetchQueryKeys(routingCollectionName);
    }

    @Override
    public synchronized List<String> getRequiredCollectionList() {
        return Arrays.asList(routingCollectionName, graphCollectionName);
    }

    @Override
    public synchronized Map<String, List<Document>> generateTestData() {
        logger.info("Generating test data in-process ...");

         Map<String, List<Document>> docs = Stream.generate(() -> UUID.randomUUID().toString())
            .limit(datagenConfig.testGraphCount)
            .flatMap(graphId -> {
                List<String> xids = Stream.generate(() -> UUID.randomUUID().toString())
                        .limit(datagenConfig.nodesPerGraph).collect(Collectors.toList());

                // create routing documents
                List<Pair<String, Document>> testDocs = xids.stream().map(xid -> {
                    Document doc = new Document();
                    doc.setId(xid);
                    doc.set(ROUTING_COLL_PROTO_FIELD, graphId);
                    return Pair.of(routingCollectionName, doc);
                }).collect(Collectors.toList());

                // create graph document
                Document graphDoc = new Document();
                graphDoc.setId(graphId);
                graphDoc.set(GRAPH_COLL_PROTO_FIELD, String.join(",", xids));
                testDocs.add(Pair.of(graphCollectionName, graphDoc));

                return testDocs.stream();})
            .collect(Collectors.groupingBy(Pair::getLeft, Collectors.mapping(t -> t.getRight(), Collectors.toList())));

        logger.info("Generating test data in-process complete.");
        return docs;
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


    // ---------- workloads ----------

    // workload-1
    @Override
    public ThrowingSupplier<SimpleResponse> lookupRoutingSingle(){
        return () -> {

            String queryXid = selectRandomDocId(idsPerPartition);
            SimpleResponse response = client.readDocument(routingCollectionName, queryXid);
            SimpleDocument doc = response.getDocuments().get(0);
            String graphId = (String) doc.properties.get(ROUTING_COLL_PROTO_FIELD);

            assert (graphId.length() > 16);
            return response;
        };
    }

    // workload-2
    @Override
    public ThrowingSupplier<SimpleResponse> lookupRoutingBatch() {
        return () -> {

            List<String> queryXids = new ArrayList<>(selectDocIdsAcrossPartitions(BATCH_QUERY_SIZE, idsPerPartition));
            SimpleResponse response = client.readDocuments(routingCollectionName, queryXids,
                    MAX_PER_PARTITION_QUERY_BATCH_SIZE);

            List<SimpleDocument> docs = response.getDocuments();
            docs.stream().forEach(d -> {
                String graphId = (String) d.properties.get(ROUTING_COLL_PROTO_FIELD);
                assert (graphId.length() > 16);
            });

            return response;
        };
    }

    // workload-3
    @Override
    public ThrowingSupplier<SimpleResponse> lookupTwoTableSingle() {
        return () -> {

            SimpleResponse response1 = lookupRoutingSingle().get();
            SimpleDocument doc1 = response1.getDocuments().get(0);
            String graphId = (String) doc1.properties.get(ROUTING_COLL_PROTO_FIELD);

            SimpleResponse response2 = client.readDocument(graphCollectionName, graphId);
            SimpleDocument doc2 =  response2.getDocuments().get(0);
            String graphDoc = (String) doc2.properties.get(GRAPH_COLL_PROTO_FIELD);

            assert (graphDoc.length() > 1);

            return new SimpleResponse(doc2, response2.getStatusCode(),
                    response1.getRuUsed() + response2.getRuUsed(),
                    response1.getRequestLatencyInMillis() + response2.getRequestLatencyInMillis(),
                    response2.getActivityId());
        };
    }

    // workload-4
    @Override
    public ThrowingSupplier<SimpleResponse> lookupTwoTableBatch() {
        return () -> {

            List<String> queryXids = new ArrayList<>(selectDocIdsAcrossPartitions(BATCH_QUERY_SIZE, idsPerPartition));

            SimpleResponse response1 = client.readDocuments(routingCollectionName, queryXids,
                    MAX_PER_PARTITION_QUERY_BATCH_SIZE);
            assert(response1.getDocuments().size() == queryXids.size());

            Set<String> graphIds = response1.getDocuments().stream()
                    .map(d -> (String) d.properties.get(ROUTING_COLL_PROTO_FIELD)).collect(Collectors.toSet());

            SimpleResponse response2  = client.readDocuments(graphCollectionName, new ArrayList<>(graphIds),
                    MAX_PER_PARTITION_QUERY_BATCH_SIZE);

            assert(response2.getDocuments().size() == graphIds.size());
            response2.getDocuments().stream().forEach(doc -> {
                String graphDoc = (String) doc.properties.get(GRAPH_COLL_PROTO_FIELD);
                assert (graphDoc.length() > 1);
            });

            return new SimpleResponse(response2.getDocuments(), response2.getStatusCode(),
                    response1.getRuUsed() + response2.getRuUsed(),
                    response1.getRequestLatencyInMillis() + response2.getRequestLatencyInMillis(),
                    response2.getActivityId());
        };
    }

}
