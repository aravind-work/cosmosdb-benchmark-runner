package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;
import com.azure.data.cosmos.*;
import com.azure.data.cosmos.internal.HttpConstants;
import com.azure.data.cosmos.internal.query.PartitionedQueryExecutionInfo;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class V3AsyncCosmosDbClient implements CosmosDbClient {

    private static final Logger logger = LoggerFactory.getLogger(V3AsyncCosmosDbClient.class.getSimpleName());

    //using for an experiment, todo: remove
    private final ExecutorService executor;

    private final CosmosDbConfig cfg;
    private final CosmosClient client;
    private final Map<String, V3DocumentDbPartitionMetadata> partitionMetadataMap = new ConcurrentHashMap<>();
    private final Map<String, CosmosContainer> CosmosContainerMap = new ConcurrentHashMap<>();
    private final PartitionedQueryExecutionInfo queryPlan;

    public V3AsyncCosmosDbClient(CosmosDbConfig cfg){
//        com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider.install();
        this.cfg = cfg;
        this.client = createDocumentClient(cfg.serviceEndpoint, cfg.masterKey, cfg.connectionMode, cfg.consistencyLevel,
                        cfg.maxPoolSize, cfg.requestTimeoutInMillis);
        this.executor = Executors.newFixedThreadPool(1000);
                //Executors.newCachedThreadPool(
                //new ThreadFactoryBuilder().setNameFormat("AsyncCosmosDbClient-%d").build());

        this.queryPlan = new PartitionedQueryExecutionInfo(
                "{\"partitionedQueryExecutionInfoVersion\":2,\"queryInfo\":{\"distinctType\":\"None\",\"top\":null,\"offset\":null,\"limit\":null,\"orderBy\":[],\"orderByExpressions\":[],\"groupByExpressions\":[],\"groupByAliases\":[],\"aggregates\":[],\"groupByAliasToAggregateType\":{},\"rewrittenQuery\":\"\",\"hasSelectValue\":false},\"queryRanges\":[{\"min\":\"\",\"max\":\"FF\",\"isMinInclusive\":true,\"isMaxInclusive\":false}]}");
    }

    // -------------  WIP

    private synchronized void loadPartitionMetadataIfNeeded(String collectionName){
        // check and update partition metadata map for collection
        if(!partitionMetadataMap.containsKey(collectionName)){
            logger.info("Fetching Partition Metadata for collection {} ...", collectionName);
            partitionMetadataMap.put(collectionName, new V3DocumentDbPartitionMetadata(client, cfg.getCollectionLink(collectionName)));
            logger.info("Fetching Partition Metadata for collection {} complete.", collectionName);
        }

        if (!CosmosContainerMap.containsKey(collectionName)) {
            logger.info("Fetching Cosmos Container {}", collectionName);
            CosmosContainer container = client.getDatabase(cfg.dbName).getContainer(collectionName).read().block().container();
            CosmosContainerMap.put(collectionName, container);
        }
    }

    private CosmosContainer getCosmosContainerOrLoad(String collectionName){
        CosmosContainer CosmosContainer = CosmosContainerMap.get(collectionName);
        if (CosmosContainer != null) {
            return CosmosContainer;
        }

        synchronized (this) {
            CosmosContainer = CosmosContainerMap.get(collectionName);

            if (CosmosContainer == null) {
                logger.info("Fetching Cosmos Container {}", collectionName);
                CosmosContainer = client.getDatabase(cfg.dbName).getContainer(collectionName).read().block().container();
                CosmosContainerMap.put(collectionName, CosmosContainer);
            }

            return CosmosContainer;
        }
    }

    @Override
    public SimpleResponse readDocuments(String collectionName, List<String> docIdList, int batchQueryMaxSize)
            throws CosmosDbException {

        final String queryString =  String.format(CosmosConstants.QUERY_STRING_BATCH,
                                                  CosmosConstants.COSMOS_DEFAULT_COLUMN_KEY, docIdList.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));


        FeedOptions options = new FeedOptions();
        options.properties(ImmutableMap.of("queryPlan", queryPlan));
        options.maxDegreeOfParallelism(100);
        options.maxBufferedItemCount(1000000);

        List<FeedResponse<CosmosItemProperties>> list = makeSync(getCosmosContainerOrLoad(collectionName).queryItems(queryString, options).collectList());

        List<SimpleDocument> docs = list.stream().flatMap(fr -> fr.results().stream()).map(properties -> new SimpleDocument(properties.id(), properties.getMap()))
                .collect(Collectors.toList());

        return new SimpleResponse(docs, 200, 0, 0, "");
    }

    public static class PojoizedJson {
        private final Map<String, String> instanceProps = new HashMap<>();

        @JsonAnyGetter
        public Map<String, String> getInstance() {
            return instanceProps;
        }

        @JsonAnySetter
        public void setProperty(String name, String value) {
            this.instanceProps.put(name, value);
        }

        @JsonIgnore
        public String getId() {
            return instanceProps.get("id");
        }

        @JsonIgnore
        public String getProperty(String propName) {
            return instanceProps.get(propName);
        }
    }


    /**
     * For Experiment, to be removed
     * @param collectionName
     * @param docIds
     * @param batchQueryMaxSize
     * @return
     */
    public SimpleResponse readDocumentsMultiThread(String collectionName, List<String> docIds, int batchQueryMaxSize) throws CosmosDbException {
        return readDocuments(collectionName, docIds, batchQueryMaxSize);
    }


    // -------------  Implementation of the Client Interface

    @Override
    public CosmosDbConfig getConfig(){ return this.cfg; }

    public Mono<CosmosItemResponse> getDocument(String collectionName, String docId){
        CosmosContainer CosmosContainer = getCosmosContainerOrLoad(collectionName);
        PartitionKey pk = getPartitionKey(docId);
        return CosmosContainer.getItem(docId, pk).read()
                .publishOn(Schedulers.immediate())
                .retryWhen(errors -> errors.flatMap(error -> {
                            // For IOExceptions, we  retry
                            if (error.getCause() instanceof RequestRateTooLargeException) {
                                RequestRateTooLargeException rrEx = (RequestRateTooLargeException) error.getCause();
                                return Mono.delay(Duration.ofMillis(rrEx.retryAfterInMilliseconds()));
                            } else {
                                logger.error("Error in getDocument()", error);
                            }

                            // For anything else, don't retry
                            return Mono.error(error);
                        })
                );
    }

    @Override
    public SimpleResponse readDocument(String collectionName, String docId) throws CosmosDbException{
        return makeSync(
                getDocument(collectionName, docId)
                    .map(r -> new SimpleResponse(new SimpleDocument(r.properties().id(),
                            r.properties().getMap()), r.statusCode(), r.requestCharge(),
                            0,//r.getRequestLatency().toMillis(),
                            r.activityId())));
    }

    public Mono<CosmosItemResponse> createDocument(String collectionName, PojoizedJson doc){
        return getCosmosContainerOrLoad(collectionName).createItem(doc, getRequestOptions(new PartitionKey(doc.getId())));
    }

    @Override
    public SimpleResponse createDocument(String collectionName, SimpleDocument sDoc) throws CosmosDbException {
        PojoizedJson doc = new PojoizedJson();
        doc.setProperty("id", sDoc.id);
        sDoc.properties.entrySet().stream().forEach(a -> doc.setProperty(a.getKey(), (String) a.getValue()));

        return makeSync(
                createDocument(collectionName, doc)
                        .map(r -> new SimpleResponse(new SimpleDocument(r.properties().id(),
                                r.properties().getMap()), r.statusCode(), r.requestCharge(),
                                0,//r.getRequestLatency().toMillis(),
                                r.activityId())));
    }

    @Override
    public void close() {
        client.close();
        executor.shutdownNow();
    }

    // -------------  Instance Helpers

    /**
     * @return For each partition in collection, returns a list of Document IDs.
     */
    public  Mono<List<List<String>>> getIdsPerPartition(String collectionName, int itemsPerPartition){
        return getIdsPerPartition(cfg.dbName, collectionName, itemsPerPartition);
    }

    public  Mono<List<List<String>>> getIdsPerPartition(String dbName, String collectionName,
                                                              int itemsPerPartition) {





        throw new NotImplementedException("");
//        String collectionLink = cfg.getCollectionLink(dbName, collectionName);
//
//        //todo :: errorHandling
//
//        Observable<List<List<String>>> keysByPartitionObs = client
//            .readPartitionKeyRanges(collectionLink, generateFeedOptions(null))
//            .retry()
//            .map(FeedResponse::results)
//            .flatMapIterable(item -> item)
//            .flatMap(pkRange ->
//                client
//                    .queryDocuments(collectionLink, generateTopNQuery(itemsPerPartition),
//                        generateFeedOptions(pkRange.getId())))
//            .map(a -> a.results().stream().map(Document::getId).collect(Collectors.toList()))
//            .toList();
//
//        return keysByPartitionObs;
    }

    public Mono<CosmosDatabaseProperties> getDatabase(){ return getDatabase(cfg.dbName);}
    public Mono<CosmosDatabaseProperties> getDatabase(String dbName) {
        Mono<CosmosDatabaseProperties> dbObs = client
            .queryDatabases(new SqlQuerySpec(CosmosConstants.ROOT_QUERY,
                    new SqlParameterList(new SqlParameter("@id", dbName))), null)
            .flatMap(feedResponse -> {
                if (feedResponse.results().isEmpty()) {
                    return Mono.error(new RuntimeException("cannot find database " + dbName));
                } else {
                    return Mono.just(feedResponse.results().get(0));
                }
            }).last();

        return dbObs;
    }

    public Mono<CosmosContainerProperties> getCollection(String collectionName) {
        return getCollection(cfg.dbName, collectionName);
    }

    public Mono<CosmosContainerProperties> getCollection(String dbName, String collectionName) {
        //todo :: errorHandling
        Mono<CosmosContainerProperties> docCollObs = client.getDatabase(dbName)
            .queryContainers(
                    new SqlQuerySpec(CosmosConstants.ROOT_QUERY,
                            new SqlParameterList(new SqlParameter("@id", collectionName))), null)
            .flatMap(feedResponse -> {
                if (feedResponse.results().isEmpty()) {
                    return Flux.error(new CosmosDbException("Cannot find collection "
                            + collectionName + "in db " + dbName + " !", null));
                } else {
                    return Flux.just(feedResponse.results().get(0));
                }
            }).last();

        return docCollObs;
    }

    public Mono<Boolean> createCollection(String collectionName, String partitionKey,
                                                int createWithRu, int postCreateRu, String consistencyLevel) {
        return createCollection(cfg.dbName, collectionName, partitionKey, createWithRu, postCreateRu, consistencyLevel);
    }

    private static IndexingPolicy getDefaultIndexingPolicy() {
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.automatic(true);
        indexingPolicy.indexingMode(IndexingMode.CONSISTENT);

        return indexingPolicy;
    }

    public Mono<Boolean> createCollection(String dbName, String collectionName, String partitionKey,
                                                int createWithRu, int postCreateRu, String consistencyLevel) {
        String databaseLink = cfg.getDatabaseLink(dbName);

        // Set Partition Definition
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        partitionKeyDefinition.paths(Arrays.asList("/" + partitionKey));

        // Set DocumentCollection Properties
        CosmosContainerProperties documentCollection = new CosmosContainerProperties(dbName, collectionName);
        documentCollection.partitionKeyDefinition(partitionKeyDefinition);
        documentCollection.indexingPolicy(getDefaultIndexingPolicy());
        //todo :: set indexing policy to exclude paths - needed for write benchmarks

        // Set RU limits to createWithRu. Note this controls the partition count.

        // Build the request
        Mono<Boolean> createStatus = client.getDatabase(dbName)
                .createContainer(documentCollection, createWithRu)
                .flatMap(response -> {
                             // Set the RU limits to postCreateRu.
//                if(response ==null && response.getResource() == null && response.getResource().getResourceId() == null){
//                    return Mono.error(new RuntimeException("Failed creating collection with name=" +
//                            collectionName + "databaseLink=" + databaseLink+". Response from " +
//                            "createCollection was null"));
//                }
                             logger.info("Successfully created collection={} databaseLink={}.", collectionName, databaseLink);

                             if (postCreateRu == createWithRu) {
                                 return Mono.just(true);
                             }

                             logger.info("Attempting to set RU post collection creation from {} to {} ...", createWithRu,
                                         postCreateRu);

                             return response.container().replaceProvisionedThroughput(postCreateRu).map(x -> true);
                         }
                );

        return createStatus;

    }

//    public Mono<Long> getCollectionSize(String collectionName) {
//        FeedOptions options = new FeedOptions();
//        options.setMaxDegreeOfParallelism(-1);
//
//        //todo :: errorHandling
//        return getCosmosContainerOrLoad(collectionName)
//            .queryItems(CosmosConstants.COUNT_QUERY, options).collectList()
//            .map(feedResponses -> {
//                return feedResponses.stream().map(fr -> fr.results()).flatMap(longs -> longs.stream()).mapToLong(Long::longValue).sum();
//            });
//    }

    public Mono<Boolean> deleteCollection(String collectionName){
        Mono<Boolean> deleteStatus = client.getDatabase(cfg.dbName).getContainer(collectionName).delete()
            .map(response -> {
                logger.info("Deleted collection {}.", collectionName);
                return true;
            })
            .onErrorResume(e -> {
                if(e instanceof CosmosClientException) {
                    CosmosClientException dce = (CosmosClientException) e;
                    if (dce.statusCode() == HttpConstants.StatusCodes.NOTFOUND) {
                        logger.warn("Collection `{}` doesn't exists. Delete operation skipped !", collectionName);
                        return Mono.just(true);
                    }
                } else {
                    logger.error("Unable to delete collection {}. Delete operation failed !", collectionName);
                }
                return Mono.error(e);
            });

        return deleteStatus;
    }

    public CosmosClient getDocumentClient(){
        return this.client;
    }

    private CosmosItemRequestOptions getRequestOptions(PartitionKey partitionKey){
        CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        options.consistencyLevel(ConsistencyLevel.valueOf(cfg.consistencyLevel.toUpperCase()));
        if (partitionKey != null) {
            options.partitionKey(partitionKey);
        }

        return options;
    }

    private CosmosItemRequestOptions getRequestOptions(){
        CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        options.consistencyLevel(ConsistencyLevel.valueOf(cfg.consistencyLevel.toUpperCase()));
        return options;
    }

    private PartitionKey getPartitionKey(String pKeyStr) {
       return new PartitionKey(pKeyStr);
    }


    // -------------  Static Helpers
    private static CosmosClient createDocumentClient(String serviceEndpoint, String masterKey,
                                                          String connectionMode, String consistencyLevel,
                                                          int maxPoolSize, int requestTimeoutInMillis){

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.requestTimeoutInMillis(requestTimeoutInMillis);

        connectionPolicy.connectionMode(ConnectionMode.valueOf(connectionMode.toUpperCase()));
        connectionPolicy.maxPoolSize(maxPoolSize);

        return CosmosClient.builder()
                .endpoint(serviceEndpoint)
                .key(masterKey)
                .connectionPolicy(connectionPolicy)
                .consistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.toUpperCase()))
                .build();
    }


    private static FeedOptions generateFeedOptions(String partitionKey) {
        throw new NotImplementedException("");
//        FeedOptions feedOptions = new FeedOptions();
//        if(partitionKey != null) {
//            feedOptions.setPartitionKeyRangeIdInternal(partitionKey);
//        }
//        feedOptions.setMaxItemCount(10000);
//        feedOptions.setMaxBufferedItemCount(10000);
//        return feedOptions;
    }

    private static String generateTopNQuery(int limit){ return "SELECT TOP " + limit + " * FROM c"; }


    // -------------  Obs
    public static <T> T makeSync(Mono<T> obs) throws CosmosDbException{
        try {
            return obs.block();
        } catch(Throwable th){

            CosmosDbException ex =  new CosmosDbException("A cosmosDB exception has occurred!", th.getCause(), false, true);
            ex.setStackTrace(th.getStackTrace());
            throw ex;
        }
    }

    public static <T> List<T> makeSyncMultiple(Flux<T> obs) throws CosmosDbException{
        try {
            return obs.collectList().block();
        } catch(Throwable th){
            CosmosDbException ex =  new CosmosDbException("A cosmosDB exception has occurred!", th.getCause(), false, true);
            ex.setStackTrace(th.getStackTrace());
            throw ex;
        }
    }

//    // --------------- Helpers
//    public static Document toDocument(SimpleDocument sDoc){
//        Document doc = new Document();
//        doc.setId(sDoc.id);
//        sDoc.properties.entrySet().stream().forEach(entry -> doc.set(entry.getKey(), entry.getValue()));
//        return doc;
//    }
//
//    public static SimpleDocument toSimpleDocument(Document doc){
//        return new SimpleDocument(doc.getId(), doc.getHashMap());
//    }

    public void verifyCollectionsExist(List<String> collectionNames){
        logger.info("Verifying that Database/Collections exists ...");
           Flux.fromIterable(collectionNames)
                .flatMap(c -> getCollection(c))
                .collectList()
                .publishOn(Schedulers.parallel())
                .doOnSuccess((x) -> logger.info("Verified that collections exists."))
                .block();
    }
}