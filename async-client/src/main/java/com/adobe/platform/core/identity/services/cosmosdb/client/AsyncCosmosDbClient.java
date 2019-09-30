package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestRateTooLargeException;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AsyncCosmosDbClient implements CosmosDbClient {

    private static final Logger logger = LoggerFactory.getLogger(AsyncCosmosDbClient.class.getSimpleName());
    private static final String COUNT_QUERY = "SELECT VALUE COUNT(1) FROM c";
    private static final String ROOT_QUERY = "SELECT * FROM root r WHERE r.id=@id";
    private static final String OFFER_QUERY_STR_FMT = "SELECT * FROM r WHERE r.offerResourceId = '%s'";
    private static final String QUERY_STRING = "SELECT * FROM root r WHERE r.%s=@id";
    private static final String QUERY_STRING_BATCH = "SELECT * FROM root r WHERE r.%s IN (%s)";

    private static final String AGGREGATE_PROPERTY = "_aggregate";
    private static final String QUERY_NAMED_PARAM_PREFIX = "@tempParam_";
    private static final String COSMOS_DEFAULT_COLUMN_KEY = "id";

    private static final int MAX_ITEM_COUNT = -1;

    //using for an experiment, todo: remove
    private final ExecutorService executor;

    private final CosmosDbConfig cfg;
    private final AsyncDocumentClient client;
    private final Map<String, DocumentDbPartitionMetadata> partitionMetadataMap = new ConcurrentHashMap<>();

    public AsyncCosmosDbClient(CosmosDbConfig cfg){
        this.cfg = cfg;
        this.client = createDocumentClient(cfg.serviceEndpoint, cfg.masterKey, cfg.connectionMode, cfg.consistencyLevel,
                        cfg.maxPoolSize);
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("AsyncCosmosDbClient-%d").build());
    }

    // -------------  WIP

    private synchronized void loadPartitionMetadataIfNeeded(String collectionName){
        // check and update partition metadata map for collection
        if(!partitionMetadataMap.containsKey(collectionName)){
            logger.info("Fetching Partition Metadata for collection {} ...", collectionName);
            partitionMetadataMap.put(collectionName, new DocumentDbPartitionMetadata(client, cfg.getCollectionLink(collectionName)));
            logger.info("Fetching Partition Metadata for collection {} complete.", collectionName);
        }
    }

    @Override
    public SimpleResponse readDocuments(String collectionName, List<String> docIdList, int batchQueryMaxSize)
                                                                                            throws CosmosDbException{
        return makeSync(
            readDocumentBatch(collectionName, docIdList, batchQueryMaxSize)
                .flatMap(o -> Observable.from(o))
                .map(f -> {
                    List<SimpleDocument> docs = f.getResults().stream()
                            .map(a -> new SimpleDocument(a.getId(), a.getHashMap())).collect(Collectors.toList());

                    int statusCode = 200; //fixme

                    double queryExecTime = f.getQueryMetrics().containsKey(QueryMetricsConstants.TotalQueryExecutionTimeInMs) ?
                        f.getQueryMetrics().get(QueryMetricsConstants.TotalQueryExecutionTimeInMs)
                                           .getTotalQueryExecutionTime().toMillis() : 0;

                    return new SimpleResponse(docs, statusCode, f.getRequestCharge(), queryExecTime, f.getActivityId());

                }).toList()
                .map( respList -> {
                    List<SimpleDocument> newDocs = new ArrayList<>();
                    SimpleResponse newResp = new SimpleResponse(newDocs, 500, 0, 0, "");
                    respList.stream().forEach(resp -> {
                        newResp.getDocuments().addAll(resp.documents);
                        newResp.ruUsed += resp.ruUsed;
                        newResp.statusCode = resp.statusCode; //fixme
                        newResp.activityId = newResp.activityId + ", " + resp.activityId;
                    });
                    return newResp;
                })
        );
    }

    Observable<List<FeedResponse<Document>>> readDocumentBatch(String collectionName, List<String> docIds,
                                                               int batchQueryMaxSize) {
        // check and update partition metadata map for collection
        if(!partitionMetadataMap.containsKey(collectionName)){
            loadPartitionMetadataIfNeeded(collectionName);
        }

        DocumentDbPartitionMetadata partitionMeta = partitionMetadataMap.get(collectionName); //todo change to name
        Map<String, Set<String>> groupedIds = partitionMeta.groupIdsByPartitionRangeId(docIds);


        List<Observable<FeedResponse<Document>>> obsList = groupedIds.entrySet().stream().flatMap(e -> {
            //Break up keys in each partition into batchQueryMaxSize
            List<String> collection = new ArrayList<>(e.getValue());
            List<List<String>> lists = new ArrayList<>();

            for (int i = 0; i < collection.size(); i += batchQueryMaxSize) {
                int end = Math.min(collection.size(), i + batchQueryMaxSize);
                lists.add(collection.subList(i, end));
            }

            return lists.stream().map(l -> Pair.of(e.getKey(), l)); })
                .map(subQuery -> {
                    //Issue a queryDocument per item emitted by stream
                    String partitionId = subQuery.getLeft();
                    List<String> queryIds = subQuery.getRight();

                    Pair<List<String>, List<SqlParameter>> tmp = createBatchSqlParameters(queryIds, QUERY_NAMED_PARAM_PREFIX);
                    SqlQuerySpec sqlQuerySpec = createSqlQuerySpec( //todo :: debug
                            QUERY_STRING_BATCH,
                            Arrays.asList(COSMOS_DEFAULT_COLUMN_KEY, StringUtils.joinWith(",", tmp.getLeft())),
                            tmp.getRight());
                    String sqlQuery = String.format(QUERY_STRING_BATCH, COSMOS_DEFAULT_COLUMN_KEY, queryIds.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
                    return client.queryDocuments(cfg.getCollectionLink(collectionName), sqlQuery, generateFeedOptions(partitionId));
                }).collect(Collectors.toList());

        return Observable.from(obsList)
                .flatMap(task -> task) //todo :: check
                .toList();
    }


    /**
     * For Experiment, to be removed
     * @param collectionName
     * @param docIds
     * @param batchQueryMaxSize
     * @return
     */
    public SimpleResponse readDocumentsMultiThread(String collectionName, List<String> docIds, int batchQueryMaxSize){
    // check and update partition metadata map for collection
        if(!partitionMetadataMap.containsKey(collectionName)){
            loadPartitionMetadataIfNeeded(collectionName);
        }

        DocumentDbPartitionMetadata partitionMeta = partitionMetadataMap.get(collectionName); //todo change to name
        Map<String, Set<String>> groupedIds = partitionMeta.groupIdsByPartitionRangeId(docIds);

        //create tasks
        List<Callable<List<FeedResponse<Document>>>> callables = groupedIds.entrySet().stream().flatMap(e -> {
            //Break up keys in each partition into batchQueryMaxSize
            List<String> collection = new ArrayList<>(e.getValue());
            List<List<String>> lists = new ArrayList<>();

            for (int i = 0; i < collection.size(); i += batchQueryMaxSize) {
                int end = Math.min(collection.size(), i + batchQueryMaxSize);
                lists.add(collection.subList(i, end));
            }

            return lists.stream().map(l -> Pair.of(e.getKey(), l)); })
        .map(subQuery -> {
            //Issue a queryDocument per item emitted by stream
            String partitionId = subQuery.getLeft();
            List<String> queryIds = subQuery.getRight();

            String sqlQuery = String.format(QUERY_STRING_BATCH, COSMOS_DEFAULT_COLUMN_KEY, queryIds.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
            Callable<List<FeedResponse<Document>>> task = () -> client.queryDocuments(cfg.getCollectionLink(collectionName), sqlQuery, generateFeedOptions(partitionId)).toList().toBlocking().single();
            return task;
        }).collect(Collectors.toList());


            List<SimpleDocument> newDocs = new ArrayList<>();
            SimpleResponse newResp = new SimpleResponse(newDocs, 500, 0, 0, "");

        //launch all tasks
        try {
            executor.invokeAll(callables)
                .stream()
                .map(future -> {
                    try {
                        return future.get();
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .flatMap(f -> f.stream().map(r -> new SimpleResponse(
                                        r.getResults().stream().map(d -> toSimpleDocument(d)).collect(Collectors.toList()),
                                        200,
                                        r.getRequestCharge(),
                                        0,
                                        r.getActivityId())))
                .forEach( resp -> {
                        newResp.getDocuments().addAll(resp.documents);
                        newResp.ruUsed += resp.ruUsed;
                        newResp.statusCode = resp.statusCode; //fixme
                        newResp.activityId = newResp.activityId + ", " + resp.activityId; });
            return newResp;
        } catch (InterruptedException e) {
        throw new RuntimeException("Thread interrupted", e);
        }
    }


    // -------------  Implementation of the Client Interface

    @Override
    public CosmosDbConfig getConfig(){ return this.cfg; }

    public Observable<ResourceResponse<Document>> getDocument(String collectionName, String docId){
        return client.readDocument(cfg.getDocumentLink(collectionName, docId), getRequestOptions(docId))
                .retryWhen(errors -> errors.flatMap(error -> {
                            // For IOExceptions, we  retry
                            if (error.getCause() instanceof RequestRateTooLargeException) {
                                RequestRateTooLargeException rrEx = (RequestRateTooLargeException) error.getCause();
                                return Observable.just(null).delay(rrEx.getRetryAfterInMilliseconds(), TimeUnit.MILLISECONDS);
                            } else {
                                logger.error("Error in getDocument()", error);
                            }

                            // For anything else, don't retry
                            return Observable.error(error);
                        })
                );
    }

    @Override
    public SimpleResponse readDocument(String collectionName, String docId) throws CosmosDbException{
        return makeSync(
                getDocument(collectionName, docId)
                    .map(r -> new SimpleResponse(new SimpleDocument(r.getResource().getId(),
                            r.getResource().getHashMap()), r.getStatusCode(), r.getRequestCharge(),
                            r.getRequestLatency().toMillis(), r.getActivityId())));
    }

    public Observable<ResourceResponse<Document>> createDocument(String collectionName, Document doc){
        RequestOptions options = getRequestOptions(doc.getId());
        return client.createDocument(cfg.getCollectionLink(collectionName), doc, options, false);
    }

    @Override
    public SimpleResponse createDocument(String collectionName, SimpleDocument sDoc) throws CosmosDbException {
        Document doc = new Document();
        doc.setId(sDoc.id);
        sDoc.properties.entrySet().stream().forEach(a -> doc.set(a.getKey(), a.getValue()));

        return makeSync(
                createDocument(collectionName, doc)
                        .map(r -> new SimpleResponse(new SimpleDocument(r.getResource().getId(),
                                r.getResource().getHashMap()), r.getStatusCode(), r.getRequestCharge(),
                                r.getRequestLatency().toMillis(), r.getActivityId())));
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
    public  Observable<List<List<String>>> getIdsPerPartition(String collectionName, int itemsPerPartition){
        return getIdsPerPartition(cfg.dbName, collectionName, itemsPerPartition);
    }

    public  Observable<List<List<String>>> getIdsPerPartition(String dbName, String collectionName,
                                                              int itemsPerPartition) {
        String collectionLink = cfg.getCollectionLink(dbName, collectionName);

        //todo :: errorHandling

        Observable<List<List<String>>> keysByPartitionObs = client
            .readPartitionKeyRanges(collectionLink, generateFeedOptions(null))
            .retry()
            .map(FeedResponse::getResults)
            .flatMapIterable(item -> item)
            .flatMap(pkRange ->
                client
                    .queryDocuments(collectionLink, generateTopNQuery(itemsPerPartition),
                        generateFeedOptions(pkRange.getId())))
            .map(a -> a.getResults().stream().map(Document::getId).collect(Collectors.toList()))
            .toList();

        return keysByPartitionObs;
    }

    public Observable<Database> getDatabase(){ return getDatabase(cfg.dbName);}
    public Observable<Database> getDatabase(String dbName) {
        Observable<Database> dbObs = client
            .queryDatabases(new SqlQuerySpec(ROOT_QUERY,
                    new SqlParameterCollection(new SqlParameter("@id", dbName))), null)
            .flatMap(feedResponse -> {
                if (feedResponse.getResults().isEmpty()) {
                    return Observable.error(new RuntimeException("cannot find database " + dbName));
                } else {
                    return Observable.just(feedResponse.getResults().get(0));
                }
            });

        return dbObs;
    }

    public Observable<DocumentCollection> getCollection(String collectionName) {
        return getCollection(cfg.dbName, collectionName);
    }

    public Observable<DocumentCollection> getCollection(String dbName, String collectionName) {
        //todo :: errorHandling
        Observable<DocumentCollection> docCollObs = client
            .queryCollections(cfg.getDatabaseLink(dbName),
                    new SqlQuerySpec(ROOT_QUERY,
                            new SqlParameterCollection(new SqlParameter("@id", collectionName))), null)
            .flatMap(feedResponse -> {
                if (feedResponse.getResults().isEmpty()) {
                    return Observable.error(new CosmosDbException("Cannot find collection "
                            + collectionName + "in db " + dbName + " !", null));
                } else {
                    return Observable.just(feedResponse.getResults().get(0));
                }
            });

        return docCollObs;
    }

    public Observable<Boolean> createCollection(String collectionName, String partitionKey,
                                                int createWithRu, int postCreateRu, String consistencyLevel) {
        return createCollection(cfg.dbName, collectionName, partitionKey, createWithRu, postCreateRu, consistencyLevel);
    }

    public Observable<Boolean> createCollection(String dbName, String collectionName, String partitionKey,
                                                int createWithRu, int postCreateRu, String consistencyLevel) {
        String databaseLink = cfg.getDatabaseLink(dbName);

        // Set Partition Definition
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        partitionKeyDefinition.setPaths(Arrays.asList("/" + partitionKey));

        // Set DocumentCollection Properties
        DocumentCollection documentCollection = new DocumentCollection();
        documentCollection.setId(collectionName);
        documentCollection.setPartitionKey(partitionKeyDefinition);
        documentCollection.setIndexingPolicy(getDefaultIndexingPolicy());
        //todo :: set indexing policy to exclude paths - needed for write benchmarks

        // Set RU limits to createWithRu. Note this controls the partition count.
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(createWithRu);
        requestOptions.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));

        // Build the request
        Observable<Boolean> createStatus = client
            .createCollection(databaseLink, documentCollection, requestOptions)
            .switchMap(response -> {
                // Set the RU limits to postCreateRu.
                if(response ==null && response.getResource() == null && response.getResource().getResourceId() == null){
                    return Observable.error(new RuntimeException("Failed creating collection with name=" +
                            collectionName + "databaseLink=" + databaseLink+". Response from " +
                            "createCollection was null"));
                }
                logger.info("Successfully created collection={} databaseLink={}.", collectionName, databaseLink);

                if(postCreateRu == createWithRu){
                    return Observable.just(true);
                }

                logger.info("Attempting to set RU post collection creation from {} to {} ...", createWithRu,
                        postCreateRu);

                return client.queryOffers(String.format(OFFER_QUERY_STR_FMT,
                    response.getResource().getResourceId()), null)
                    .switchMap(page -> Observable.from(page.getResults()))
                    .first()
                    .switchMap(offer -> {
                        offer.setThroughput(postCreateRu);
                        return client.replaceOffer(offer);
                    })
                    .map(newOffer -> {
                        logger.info("Successfully changed RU from {} to {} for collection={} databaseLink={}.",
                                createWithRu, postCreateRu, collectionName, databaseLink);
                        if(newOffer.getResource().getThroughput() != postCreateRu){
                            Observable.error(new RuntimeException("Failed to update RU Offer from {} to {} for " +
                                    "collection=" + collectionName + " databaseLink= + " + databaseLink + "."));
                        }
                        return true;
                    })
                    .onErrorResumeNext(e ->
                            Observable.error(new RuntimeException("Failed to update RU offer from " + createWithRu +
                                    " to " + postCreateRu + " for collection " +
                                    "with name=" + collectionName + " databaseLink=" + databaseLink +
                                    ".", e)));
            })
            .onErrorResumeNext(e ->
                    Observable.error(new RuntimeException("Failed to create collection with name=name=" +
                            collectionName + " databaseLink=" + databaseLink +".", e)));

        // Add deferred logging statement to the observable
        //logger.info("Creating collection with name={} databaseLink={} ...", collectionName, databaseLink);
        Observable<Boolean> logObs = Observable.defer(() -> {
            logger.info("Creating collection with name={} databaseLink={} ...", collectionName, databaseLink);
            return Observable.just(true);});

        return logObs.mergeWith(createStatus);
    }

    public Observable<Long> getCollectionSize(String collectionName) {
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);

        //todo :: errorHandling
        return client
            .queryDocuments(cfg.getCollectionLink(collectionName), COUNT_QUERY, options)
            .flatMap(feedResponse -> {
                if(feedResponse.getResults().isEmpty()){
                    return Observable.just(0L);
                } else {
                    return Observable.just(feedResponse.getResults().get(0).getLong(AGGREGATE_PROPERTY));
                }
            });
    }

    public Observable<Boolean> deleteCollection(String collectionName){
        Observable<Boolean> deleteStatus = client
            .deleteCollection(cfg.getCollectionLink(collectionName), new RequestOptions())
            .map(response -> {
                logger.info("Deleted collection {}.", collectionName);
                return true;
            })
            .onErrorResumeNext(e -> {
                if(e instanceof DocumentClientException) {
                    DocumentClientException dce = (DocumentClientException) e;
                    if (dce.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
                        logger.warn("Collection `{}` doesn't exists. Delete operation skipped !", collectionName);
                        return Observable.just(true);
                    }
                } else {
                    logger.error("Unable to delete collection {}. Delete operation failed !", collectionName);
                }
                return Observable.error(e);
            });

        return deleteStatus;
    }

    public AsyncDocumentClient getDocumentClient(){
        return this.client;
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


    // -------------  Static Helpers
    private static AsyncDocumentClient createDocumentClient(String serviceEndpoint, String masterKey,
                                                            String connectionMode, String consistencyLevel,
                                                            int maxPoolSize){

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.valueOf(connectionMode));
        connectionPolicy.setMaxPoolSize(maxPoolSize);

        return new AsyncDocumentClient.Builder()
                .withServiceEndpoint(serviceEndpoint)
                .withMasterKeyOrResourceToken(masterKey)
                .withConnectionPolicy(connectionPolicy)
                .withConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel))
                .build();
    }

    private static IndexingPolicy getDefaultIndexingPolicy() {
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(false);
        indexingPolicy.setIndexingMode(IndexingMode.None);

        return indexingPolicy;
    }

    private static FeedOptions generateFeedOptions(String partitionKey) {
        FeedOptions feedOptions = new FeedOptions();
        if(partitionKey != null) {
            feedOptions.setPartitionKeyRangeIdInternal(partitionKey);
        }
        feedOptions.setMaxItemCount(10000);
        feedOptions.setMaxBufferedItemCount(10000);
        return feedOptions;
    }

    private static String generateTopNQuery(int limit){ return "SELECT TOP " + limit + " * FROM c"; }


    /**
     * Creates SqlQuerySpec given the inputs
     *   Formats query based on list of replacements
     *   Creates SqlCollection based on list of SqlParameters
     *
     * @param sqlQuery       - SQL Query to format
     * @param sqlArgs        - List of Arguments to replace in Query
     * @param sqlParameters  - List of SqlParameters to substitute into Query
     * @return SqlQuerySpec
     */
    SqlQuerySpec createSqlQuerySpec(String sqlQuery, List<String> sqlArgs, List<SqlParameter> sqlParameters) {
        String query = String.format(sqlQuery, sqlArgs.toArray());
        SqlParameterCollection sqlParameterCollection = new SqlParameterCollection(sqlParameters);
        SqlQuerySpec sqlQuerySpec = new SqlQuerySpec(query, sqlParameterCollection);
        return sqlQuerySpec;
    }

    /**
     * Creates a list of temp parameters to populate into the IN clause of the query based on number of keys
     *   ex. we have 3 keys to lookup
     * Select ... FROM .... WHERE r.id IN (%s) =>
     *       r.id IN (@tempParam_1, @tempParam_2, @tempParam_3)
     *
     * Also creates list of SqlParameters to replace temp parameters
     * ex.
     * SqlParameter(@tempParam_1, keys(0))
     * SqlParameter(@tempParam_2, keys(1))
     * SqlParameter(@tempParam_3, keys(2))
     */
    Pair< List<String>,List<SqlParameter> > createBatchSqlParameters(List<String> keys, String paramPrefix) {

        List<String> paramNameList = IntStream
                .range(0, keys.size())
                .mapToObj(Integer::toString)
                .map(i -> paramPrefix + i)
                .collect(Collectors.toList());

        List<SqlParameter> sqlParameterList = IntStream
                .range(0, paramNameList.size())
                .mapToObj(paramNameIdx -> new SqlParameter(paramNameList.get(paramNameIdx), keys.get(paramNameIdx)))
                .collect(Collectors.toList());


        return Pair.of(paramNameList, sqlParameterList);
    }


    // -------------  Obs
    //todo:: which scheduler
    public static <T> T makeSync(Observable<T> obs) throws CosmosDbException{
        try {
            return obs.toBlocking().first();
        } catch(Throwable th){
            CosmosDbException ex =  new CosmosDbException("A cosmosDB exception has occurred!", th.getCause(), false, true);
            ex.setStackTrace(th.getStackTrace());
            throw ex;
        }
    }

    public static <T> List<T> makeSyncMultiple(Observable<T> obs) throws CosmosDbException{
        try {
            return obs.toList().toBlocking().single();
        } catch(Throwable th){
            CosmosDbException ex =  new CosmosDbException("A cosmosDB exception has occurred!", th.getCause(), false, true);
            ex.setStackTrace(th.getStackTrace());
            throw ex;
        }
    }

    public static Document toDocument(SimpleDocument sDoc){
        Document doc = new Document();
        doc.setId(sDoc.id);
        sDoc.properties.entrySet().stream().forEach(entry -> doc.set(entry.getKey(), entry.getValue()));
        return doc;
    }

    public static SimpleDocument toSimpleDocument(Document doc){
        return new SimpleDocument(doc.getId(), doc.getHashMap());
    }

}