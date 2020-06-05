package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;
import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.ItemOperationsBridge;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.RequestRateTooLargeException;
import com.azure.cosmos.implementation.apachecommons.lang.tuple.Pair;
import com.azure.cosmos.implementation.guava25.collect.ImmutableMap;
import com.azure.cosmos.implementation.query.PartitionedQueryExecutionInfo;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosDatabaseProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.IndexingMode;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.ModelBridgeInternal;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.models.QueryRequestOptions;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
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
import java.util.stream.Stream;

public class V4AsyncCosmosDbClient implements CosmosDbClient {

    private static final Logger logger = LoggerFactory.getLogger(V4AsyncCosmosDbClient.class.getSimpleName());

    //using for an experiment, todo: remove
    private final ExecutorService executor;

    private final CosmosDbConfig cfg;
    private final CosmosAsyncClient client;
    private final Map<String, V4DocumentDbPartitionMetadata> partitionMetadataMap = new ConcurrentHashMap<>();
    private final Map<String, CosmosAsyncContainer> cosmosAsyncContainerMap = new ConcurrentHashMap<>();
    private final PartitionedQueryExecutionInfo queryPlan;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
            .configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true)
            .configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true)
            .registerModule(new AfterburnerModule());

    
    public V4AsyncCosmosDbClient(CosmosDbConfig cfg){
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
            partitionMetadataMap.put(collectionName, new V4DocumentDbPartitionMetadata(client, cfg.getCollectionLink(collectionName)));
            logger.info("Fetching Partition Metadata for collection {} complete.", collectionName);
        }

        if (!cosmosAsyncContainerMap.containsKey(collectionName)) {
            logger.info("Fetching Cosmos Container {}", collectionName);
            CosmosAsyncContainer container = client.getDatabase(cfg.dbName).getContainer(collectionName);
            container.read().block();
            cosmosAsyncContainerMap.put(collectionName, container);
        }
    }

    private CosmosAsyncContainer getCosmosContainerOrLoad(String collectionName){
        CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncContainerMap.get(collectionName);
        if (cosmosAsyncContainer != null) {
            return cosmosAsyncContainer;
        }

        synchronized (this) {
            cosmosAsyncContainer = cosmosAsyncContainerMap.get(collectionName);

            if (cosmosAsyncContainer == null) {
                logger.info("Fetching Cosmos Container {}", collectionName);
                cosmosAsyncContainer = client.getDatabase(cfg.dbName).getContainer(collectionName);
                cosmosAsyncContainer.read().block();
                cosmosAsyncContainerMap.put(collectionName, cosmosAsyncContainer);
            }

            return cosmosAsyncContainer;
        }
    }

    @Override
    public SimpleResponse readDocuments(String collectionName, List<String> docIdList, int batchQueryMaxSize)
            throws CosmosDbException {

        QueryRequestOptions options = new QueryRequestOptions();

        ModelBridgeInternal.setQueryRequestOptionsProperties(options, ImmutableMap.of("queryPlan", queryPlan));
        options.setMaxDegreeOfParallelism(100);
        options.setMaxBufferedItemCount(1000000);


        FeedResponse<JsonNode> results = ItemOperationsBridge.readManyAsync(getCosmosContainerOrLoad(collectionName), docIdList.stream().map(id -> Pair.of(id, new PartitionKey(id))).collect(Collectors.toList()), options, JsonNode.class).block();
        Stream<SimpleDocument> resultStream = results.getElements().stream().map(r -> objectMapper.convertValue(r, HashMap.class)).map(map -> new SimpleDocument((String) map.get("id"), map));

        List<SimpleDocument> docs = resultStream.collect(Collectors.<SimpleDocument>toList());
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

    public Mono<CosmosItemResponse<JsonNode>> getDocument(String collectionName, String docId){
        CosmosAsyncContainer cosmosAsyncContainer = getCosmosContainerOrLoad(collectionName);
        PartitionKey pk = getPartitionKey(docId);
        return cosmosAsyncContainer.readItem(docId, pk, JsonNode.class)
                .publishOn(Schedulers.immediate())
                .retryWhen(errors -> errors.flatMap(error -> {
                            // For IOExceptions, we  retry
                            if (error.getCause() instanceof RequestRateTooLargeException) {
                                RequestRateTooLargeException rrEx = (RequestRateTooLargeException) error.getCause();
                                return Mono.delay(rrEx.getRetryAfterDuration());
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
                     .map(rr -> {
                         JsonNode r = rr.getItem();
                         //return getMapper().convertValue(this.propertyBag, HashMap.class);
                         return new SimpleResponse(new SimpleDocument(r.get("id").textValue(),
                                 objectMapper.convertValue(r, HashMap.class)), rr.getStatusCode(), rr.getRequestCharge(),
                                 0,//r.getRequestLatency().toMillis(),
                                 rr.getActivityId());
                     }));
    }

    public Mono<CosmosItemResponse<PojoizedJson>> createDocument(String collectionName, PojoizedJson doc){
        return getCosmosContainerOrLoad(collectionName).createItem(doc, new PartitionKey(doc.getId()), getRequestOptions());
    }

    @Override
    public SimpleResponse createDocument(String collectionName, SimpleDocument sDoc) throws CosmosDbException {
        PojoizedJson doc = new PojoizedJson();
        doc.setProperty("id", sDoc.id);
        sDoc.properties.entrySet().stream().forEach(a -> doc.setProperty(a.getKey(), (String) a.getValue()));

        return makeSync(
                createDocument(collectionName, doc)
                        .map(rr -> {
                            PojoizedJson r = rr.getItem();

                            //return getMapper().convertValue(this.propertyBag, HashMap.class);
                            return new SimpleResponse(new SimpleDocument(r.getId(),
                                    objectMapper.convertValue(r, HashMap.class)), rr.getStatusCode(), rr.getRequestCharge(),
                                    0,//r.getRequestLatency().toMillis(),
                                    rr.getActivityId());
                        }));
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
//            .map(FeedResponse::getResults)
//            .flatMapIterable(item -> item)
//            .flatMap(pkRange ->
//                client
//                    .queryDocuments(collectionLink, generateTopNQuery(itemsPerPartition),
//                        generateFeedOptions(pkRange.getId())))
//            .map(a -> a.getResults().stream().map(Document::getId).collect(Collectors.toList()))
//            .toList();
//
//        return keysByPartitionObs;
    }

    public Mono<CosmosDatabaseProperties> getDatabase(){ return getDatabase(cfg.dbName);}
    public Mono<CosmosDatabaseProperties> getDatabase(String dbName) {
        Mono<CosmosDatabaseProperties> dbObs = client
            .queryDatabases(new SqlQuerySpec(CosmosConstants.ROOT_QUERY, (new SqlParameter("@id", dbName))), null)
                .byPage()
            .flatMap(feedResponse -> {
                if (feedResponse.getResults().isEmpty()) {
                    return Mono.error(new RuntimeException("cannot find database " + dbName));
                } else {
                    return Mono.just(feedResponse.getResults().get(0));
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
                    new SqlQuerySpec(CosmosConstants.ROOT_QUERY, new SqlParameter("@id", collectionName)), null)
                .byPage()
            .flatMap(feedResponse -> {
                if (feedResponse.getResults().isEmpty()) {
                    return Flux.error(new CosmosDbException("Cannot find collection "
                            + collectionName + "in db " + dbName + " !", null));
                } else {
                    return Flux.just(feedResponse.getResults().get(0));
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
        indexingPolicy.setAutomatic(true);
        indexingPolicy.setIndexingMode(IndexingMode.CONSISTENT);

        return indexingPolicy;
    }

    public Mono<Boolean> createCollection(String dbName, String collectionName, String partitionKey,
                                                int createWithRu, int postCreateRu, String consistencyLevel) {
        String databaseLink = cfg.getDatabaseLink(dbName);

        // Set Partition Definition
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        partitionKeyDefinition.setPaths(Arrays.asList("/" + partitionKey));

        // Set DocumentCollection Properties
        CosmosContainerProperties documentCollection = new CosmosContainerProperties(dbName, collectionName);
        documentCollection.setPartitionKeyDefinition(partitionKeyDefinition);
        documentCollection.setIndexingPolicy(getDefaultIndexingPolicy());
        //todo :: set indexing policy to exclude paths - needed for write benchmarks

        // Set RU limits to createWithRu. Note this controls the partition count.

        // Build the request
        Mono<Boolean> createStatus = client.getDatabase(dbName)
                .createContainer(documentCollection, ThroughputProperties.createManualThroughput(createWithRu))
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


                             return client.getDatabase(dbName).getContainer(documentCollection.getId())
                                    .replaceThroughput(ThroughputProperties.createManualThroughput(postCreateRu))
                                    .map(x -> true);

                         }
                );

        return createStatus;

    }

    public Mono<Long> getCollectionSize(String collectionName) {
        QueryRequestOptions options = new QueryRequestOptions();
        options.setMaxDegreeOfParallelism(-1);

        //todo :: errorHandling
        return getCosmosContainerOrLoad(collectionName)
            .queryItems(CosmosConstants.COUNT_QUERY, options, Long.class).byPage().collectList()
            .map(feedResponses -> {
                return feedResponses.stream().map(fr -> fr.getResults()).flatMap(longs -> longs.stream()).mapToLong(Long::longValue).sum();
            });
    }

    public Mono<Boolean> deleteCollection(String collectionName){
        Mono<Boolean> deleteStatus = client.getDatabase(cfg.dbName).getContainer(collectionName).delete()
            .map(response -> {
                logger.info("Deleted collection {}.", collectionName);
                return true;
            })
            .onErrorResume(e -> {
                if(e instanceof CosmosException) {
                    CosmosException dce = (CosmosException) e;
                    if (dce.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
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

    public CosmosAsyncClient getDocumentClient(){
        return this.client;
    }

    private CosmosItemRequestOptions getRequestOptions(){
        CosmosItemRequestOptions options = new CosmosItemRequestOptions();
        // TODO: dont' support
         //options.setConsistencyLevel(ConsistencyLevel.valueOf(cfg.consistencyLevel.toUpperCase()));
        return options;
    }

    private PartitionKey getPartitionKey(String pKeyStr) {
       return new PartitionKey(pKeyStr);
    }


    // -------------  Static Helpers
    private static CosmosAsyncClient createDocumentClient(String serviceEndpoint, String masterKey,
                                                          String connectionMode, String consistencyLevel,
                                                          int maxPoolSize, int requestTimeoutInMillis){


        ConnectionMode mode = ConnectionMode.valueOf(connectionMode.toUpperCase());

        if (mode != ConnectionMode.DIRECT) {
            throw new NotImplementedException(connectionMode);
        }

        DirectConnectionConfig config = DirectConnectionConfig.getDefaultConfig();
//        config.setRequestTimeout(Duration.ofMillis(requestTimeoutInMillis));

//        connectionPolicy.setMaxPoolSize(maxPoolSize);

        return new CosmosClientBuilder()
                .endpoint(serviceEndpoint)
                .key(masterKey)
                .directMode(config)
                .consistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.toUpperCase()))
                .buildAsyncClient();
    }


    private static QueryRequestOptions generateFeedOptions(String partitionKey) {
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

    public static <T> Iterable<FeedResponse<T>> makeSync(CosmosPagedFlux<T> obs) throws CosmosDbException{
        try {
            return obs.byPage().toIterable();
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