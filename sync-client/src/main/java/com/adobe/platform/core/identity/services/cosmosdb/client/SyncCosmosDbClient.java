package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;
import com.microsoft.azure.documentdb.*;
import io.vavr.control.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SyncCosmosDbClient implements CosmosDbClient {
    private static final Logger logger = LoggerFactory.getLogger(SyncCosmosDbClient.class.getSimpleName());
    private final ExecutorService executor; //for batch queries

    private final CosmosDbConfig cfg;
    private final DocumentClient client;
    private final Map<String, DocumentDbPartitionMetadataSync> partitionMetadataMap = new ConcurrentHashMap<>();

    public SyncCosmosDbClient(CosmosDbConfig cfg) {
        this.cfg = cfg;
        this.client = createDocumentClient(cfg.serviceEndpoint, cfg.masterKey, cfg.connectionMode, cfg.consistencyLevel,
                cfg.maxPoolSize);
        this.executor = Executors.newFixedThreadPool(cfg.batchQueryExecutorThreadPoolSize);
    }

    private DocumentClient createDocumentClient(String serviceEndpoint, String masterKey, String connectionMode, String consistencyLevel, int maxPoolSize) {
        ConnectionPolicy cPolicy = new ConnectionPolicy();

        if(connectionMode.equalsIgnoreCase("Direct")) {
            cPolicy.setConnectionMode(ConnectionMode.DirectHttps);
        } else {
            cPolicy.setConnectionMode(ConnectionMode.Gateway);
        }
        cPolicy.setMaxPoolSize(maxPoolSize);
        cPolicy.setRequestTimeout(cfg.requestTimeoutInMillis/1000);
        cPolicy.setDirectRequestTimeout(cfg.requestTimeoutInMillis/1000);

        return new DocumentClient(serviceEndpoint, masterKey, cPolicy, ConsistencyLevel.valueOf(consistencyLevel));
    }

    private synchronized void loadPartitionMetadataIfNeeded(String collectionName) throws DocumentClientException {
        // check and update partition metadata map for collection
        if(!partitionMetadataMap.containsKey(collectionName)){
            logger.info("Fetching Partition Metadata for collection {} ...", collectionName);
            partitionMetadataMap.put(collectionName, new DocumentDbPartitionMetadataSync(client, cfg.getCollectionLink(collectionName)));
            logger.info("Fetching Partition Metadata for collection {} complete.", collectionName);
        }
    }

    @Override
    public SimpleResponse readDocument(String collectionName, String docId) throws CosmosDbException {
        try {
            ResourceResponse<Document> r = client.readDocument(cfg.getDocumentLink(collectionName, docId), getRequestOptions(docId));
            return new SimpleResponse(new SimpleDocument(r.getResource().getId(),
                    r.getResource().getHashMap()), r.getStatusCode(), r.getRequestCharge(),
                    0,//r.getRequestLatency().toMillis(),
                    r.getActivityId());
        } catch (DocumentClientException e){
            throw new CosmosDbException(e);
        }
    }

    @Override
    public SimpleResponse createDocument(String collectionName, SimpleDocument sDoc) throws CosmosDbException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleResponse readDocuments(String collectionName, List<String> idList, int batchQueryMaxSize) throws CosmosDbException {
        // check and update partition metadata map for collection
        try {
            if (!partitionMetadataMap.containsKey(collectionName)) {
                loadPartitionMetadataIfNeeded(collectionName);
            }
        } catch(DocumentClientException e){
            throw new CosmosDbException(e);
        }

        DocumentDbPartitionMetadataSync metadata = partitionMetadataMap.get(collectionName); //todo change to name
        HashMap<String, Set<String>> partitionKeyIdsMap = metadata.getIdsByPartition(idList);
        int numPartitions = partitionKeyIdsMap.keySet().size();

        if (numPartitions < 1) {
            logger.error("Unable to map document ids to partition keys.");
            throw new CosmosDbException("InvalidInputIds");
        }

        // If the query is going to one partition, we don't need to use a different thread
        if (numPartitions < 2) {
            String partitionKey = partitionKeyIdsMap.keySet().iterator().next();
            final String queryString =  String.format(CosmosConstants.QUERY_STRING_BATCH, CosmosConstants.COSMOS_DEFAULT_COLUMN_KEY, idList.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
            final FeedOptions queryOptions = getFeedOptions(partitionKey);

            try {
                FeedResponse<Document> resp = client.queryDocuments(metadata.getCollectionLink(), queryString, queryOptions);
                List<SimpleDocument> sDocList = resp.getQueryIterable().toList().stream().map(this::toSimpleDocument).collect(Collectors.toList());
                SimpleResponse result = new SimpleResponse(sDocList, 200, resp.getRequestCharge() ,0 ,resp.getActivityId());
                return result;
            } catch (Exception e) {
                throw new CosmosDbException(e);
            }
        } else {

            SimpleResponse result = new SimpleResponse(new ArrayList<>(), 200, 0 ,0 ,"");
            CountDownLatch latch = new CountDownLatch(numPartitions);

            AtomicInteger exceptionCount = new AtomicInteger(0);
            AtomicInteger documentDbExceptionCount = new AtomicInteger(0);

            for (String partitionKey : partitionKeyIdsMap.keySet()) {
                executor.submit(() -> {
                    // Use the querySpecFunction to crete an SqlQuerySpec for this subset of ids
                    final String queryString =  String.format(CosmosConstants.QUERY_STRING_BATCH, CosmosConstants.COSMOS_DEFAULT_COLUMN_KEY, partitionKeyIdsMap.get(partitionKey).stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
                    final FeedOptions queryOptions = getFeedOptions(partitionKey);

                    try {
                        FeedResponse<Document> partialQueryResult = client.queryDocuments(metadata.getCollectionLink(), queryString, queryOptions);
                        List<SimpleDocument> sDocList = partialQueryResult.getQueryIterable().toList().stream().map(this::toSimpleDocument).collect(Collectors.toList());

                        synchronized (result) {
                            result.getDocuments().addAll(sDocList);
                            result.ruUsed += partialQueryResult.getRequestCharge();
                            result.activityId += partialQueryResult.getActivityId() + ", ";
                        }

                    } catch (Exception ex) {
                        return Either.left(new Exception("documentDbExceptionCount = " +
                                documentDbExceptionCount.longValue() + "exceptionCount = " +
                                exceptionCount.longValue(), ex));
                    }
                    latch.countDown();
                    return true;
                });
            }

            // true if countdown finished, false if time limit reached
            try {
                final boolean complete = latch.await(cfg.batchQueryTimeoutInMillis, TimeUnit.MILLISECONDS);
                if (!complete) {
                    throw new CosmosDbException("Batch query did not complete for timeout period(ms) of " + cfg.batchQueryTimeoutInMillis);
                }
            } catch (InterruptedException e) {
                throw new CosmosDbException("Exception during latch await", e);
            }

            //TODO: what status code to return if query resulted in varied errors from different partitions
            if (documentDbExceptionCount.get() > 0) {
                throw new CosmosDbException("Document client exception encountered while performing batch query");
            }

            if (exceptionCount.get() > 0) {
                throw new CosmosDbException("Runtime exception encountered while performing batch query");
            }

            return result;
        }
    }

    @Override
    public SimpleResponse readDocumentsMultiThread(String collectionName, List<String> docIdList, int batchQueryMaxSize) throws CosmosDbException {
        return readDocuments(collectionName, docIdList, batchQueryMaxSize); //For sync client there is no different
    }


    @Override
    public CosmosDbConfig getConfig() {
        return this.cfg;
    }

    @Override
    public void close() {
        client.close();
        executor.shutdownNow();
    }

    private RequestOptions getRequestOptions(){ return getRequestOptions(null);}
    private RequestOptions getRequestOptions(String pKeyStr) {
        RequestOptions options = new RequestOptions();
        options.setConsistencyLevel(ConsistencyLevel.valueOf(cfg.consistencyLevel));
        if (pKeyStr != null) {
            PartitionKey partitionKey = new PartitionKey(pKeyStr);
            options.setPartitionKey(partitionKey);
        }
        return options;
    }

    private static FeedOptions getFeedOptions(String partitionKey) {
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setPageSize(-1);
        queryOptions.setPartitionKeyRangeIdInternal(partitionKey);
        return queryOptions;
    }


    public  SimpleDocument toSimpleDocument(Document doc){
        return new SimpleDocument(doc.getId(), doc.getHashMap());
    }
}
