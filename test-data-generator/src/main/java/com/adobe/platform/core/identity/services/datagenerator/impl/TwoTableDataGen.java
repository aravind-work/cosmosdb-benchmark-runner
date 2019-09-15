package com.adobe.platform.core.identity.services.datagenerator.impl;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.AbstractDataGen;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import com.microsoft.azure.cosmosdb.Document;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TwoTableDataGen extends AbstractDataGen {
    Logger logger = LoggerFactory.getLogger(TwoTableDataGen.class.getSimpleName());

    public static final String ROUTING_COLL_PROTO_FIELD = "rp";
    public static final String GRAPH_COLL_PROTO_FIELD = "gp";

    private final String routingCollectionName;
    private final String graphCollectionName;

    public TwoTableDataGen(DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        super(datagenConfig, cosmosConfig);
        routingCollectionName = getRoutingCollectionName(datagenConfig);
        graphCollectionName = getGraphCollectionName(datagenConfig);
    }

    // ---------- Interface methods ----------

    @Override
    public void shutdown(){
        client.close();
    }

    @Override
    public List<String> getRequiredCollectionList() {
        return TwoTableDataGen.getRequiredCollectionListStatic(datagenConfig);
    }

    public static List<String> getRequiredCollectionListStatic(DataGenConfig datagenConfig) {
        String routingCollectionName = datagenConfig.collectionPrefix + "routing";
        String graphCollectionName = datagenConfig.collectionPrefix + "graph";
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

    public static String getRoutingCollectionName(DataGenConfig cfg){
        return cfg.collectionPrefix + "routing";
    }

    public static  String getGraphCollectionName(DataGenConfig cfg){
        return cfg.collectionPrefix + "graph";
    }
}
