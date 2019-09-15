package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.workload;

import com.adobe.platform.core.identity.services.cosmosdb.client.*;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen.GRAPH_COLL_PROTO_FIELD;
import static com.adobe.platform.core.identity.services.datagenerator.impl.TwoTableDataGen.ROUTING_COLL_PROTO_FIELD;

public class TwoTableWorkload extends AbstractWorkload{
    Logger logger = LoggerFactory.getLogger(AbstractWorkload.class.getSimpleName());

    private final CosmosDbConfig cosmosConfig;
    private final String routingCollectionName;
    private final String graphCollectionName;
    private final CosmosDbClientType clientType;

    private CosmosDbClient client;
    private List<List<String>> idsPerPartition;

    public TwoTableWorkload(CosmosDbConfig cosmosConfig, String cosmosDbCollectionPrefix, CosmosDbClientType clientType) {
        this.cosmosConfig = cosmosConfig;
        this.clientType = clientType;

        routingCollectionName = cosmosDbCollectionPrefix + "routing";
        graphCollectionName = cosmosDbCollectionPrefix + "graph";
    }

    public synchronized void workloadSetup(){
        AsyncCosmosDbClient asyncClient = new AsyncCosmosDbClient(cosmosConfig);

        // These function only work with async client for now. todo
        asyncClient.verifyCollectionsExist(Arrays.asList(routingCollectionName, graphCollectionName));

        // Get keys from each partition for querying
        idsPerPartition = asyncClient.getIdsPerPartition(routingCollectionName, MAX_IDS_TO_FETCH_PER_PARTITION)
                        .toBlocking().single();
        int partitionCount = idsPerPartition.size();
        long docCount = idsPerPartition.stream().flatMap(List::stream).count();
        logger.info("Fetched {} ids from {} partitions.", docCount, partitionCount);

        // If we are benchmarking the sync SDK, close the Async Client and init the sync client
        if(clientType.equals(CosmosDbClientType.SYNC)){
            asyncClient.close();
            logger.info("Using SYNC client for this workload");
            client = new SyncCosmosDbClient(cosmosConfig);
        } else {
            logger.info("Using ASYNC client for this workload");
            client = asyncClient;
        }
    }

    @Override
    public void workloadTeardown() {
        try {
            client.close();
            Thread.sleep(5000); //Async client has stray (netty) threads after calling close. //todo
        } catch (InterruptedException e) {
            e.printStackTrace();
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

    // workload-2.1 (this is for experimentation only)
    @Override
    public ThrowingSupplier<SimpleResponse> lookupRoutingBatchMultiThread() {
        return () -> {

            List<String> queryXids = new ArrayList<>(selectDocIdsAcrossPartitions(BATCH_QUERY_SIZE, idsPerPartition));
            SimpleResponse response = client.readDocumentsMultiThread(routingCollectionName, queryXids,
                    MAX_PER_PARTITION_QUERY_BATCH_SIZE);

            List<SimpleDocument> docs = response.getDocuments();

            assert (docs.size() == BATCH_QUERY_SIZE);

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

            Set<String> xids = response2.getDocuments().stream().flatMap(doc -> {
                String graphDoc = (String) doc.properties.get(GRAPH_COLL_PROTO_FIELD);
                assert (graphDoc.length() > 1);
                return Arrays.asList(graphDoc.split(",")).stream();
            }).collect(Collectors.toSet());
            assert(xids.containsAll(queryXids));

            return new SimpleResponse(response2.getDocuments(), response2.getStatusCode(),
                    response1.getRuUsed() + response2.getRuUsed(),
                    response1.getRequestLatencyInMillis() + response2.getRequestLatencyInMillis(),
                    response2.getActivityId());
        };
    }

}