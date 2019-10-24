package com.adobe.platform.core.identity.services.datagenerator.impl;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import com.adobe.platform.identity.services.instagraph.manager.GraphManagerEnum;
import com.adobe.platform.identity.services.instagraph.manager.GraphManagerSettings;
import com.adobe.platform.identity.services.instagraph.metrics.MetricSettings;
import com.adobe.platform.identity.services.instagraph.metrics.MetricsEnum;
import com.adobe.platform.identity.services.instagraph.serde.SerDeType;
import com.adobe.platform.identity.services.instagraph.store.cosmos.CosmosSettings;
import com.adobe.platform.identity.services.instagraph.util.CommonXidParser$;
import com.adobe.platform.identity.services.instagraph.v2.datamodel.Graph;
import com.adobe.platform.identity.services.instagraph.v2.datamodel.IdGraphGeneratorV2;
import com.adobe.platform.identity.services.instagraph.v2.manager.GraphManagerFactoryNew;
import com.adobe.platform.identity.services.instagraph.v2.manager.GraphManagerNew;
import com.adobe.platform.identity.services.instagraph.v2.protobuf.GraphUpdateProto;
import com.adobe.platform.identity.services.instagraph.v2.util.GraphManagerNewUtil;
import com.codahale.metrics.MetricRegistry;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Enumeration;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class InstagraphDataGen extends TwoTableDataGen {

    Logger logger = LoggerFactory.getLogger(InstagraphDataGen.class.getSimpleName());
    private static final int INGEST_PARALLELISM = 100;
    private static final int[] DEFAULT_REALMS = new int[]{1111};

    public InstagraphDataGen(DataGenConfig datagenConfig, CosmosDbConfig cosmosConfig){
        super(datagenConfig, cosmosConfig);
    }

    @Override
    public Map<String, List<Document>> generateTestData() {
        GraphManagerNew gManager = createGraphManager(cosmosConfig, datagenConfig);

        ForkJoinPool forkJoinPool = new ForkJoinPool(INGEST_PARALLELISM);
        AtomicInteger counter = new AtomicInteger();
        try {
            forkJoinPool.submit(() ->
                    Stream.generate(() -> UUID.randomUUID().toString())
                            .limit(datagenConfig.testGraphCount)
                            .parallel()
                            .forEach(graphId -> {
                                Graph g = IdGraphGeneratorV2.generateGraph(datagenConfig.nodesPerGraph,
                                        (datagenConfig.nodesPerGraph * (datagenConfig.nodesPerGraph - 1)) / 2,
                                        CommonXidParser$.MODULE$,
                                        Option.apply(DEFAULT_REALMS));
                                GraphUpdateProto graphUpdateProto = GraphManagerNewUtil.generateGraphUpdateProtoFromGraph(g);
                                gManager.ingest(graphUpdateProto);

                                int currVal = counter.incrementAndGet();
                                if(currVal % 1000 == 0){
                                    logger.info("Processed {}/{} documents.", currVal, datagenConfig.testGraphCount);
                                }
                            })
            ).get();
        } catch (InterruptedException e) {
            logger.error("Exception", e);
        } catch (ExecutionException e) {
            logger.error("Exception", e);
        } finally {
            gManager.shutdown();
        }

        // All ingestion is done above
        return Collections.emptyMap();
    }

    @Override
    public boolean validateTestData() {
        return true;
    }

    @Override
    public void shutdown(){
        client.close();
    }

    private GraphManagerNew createGraphManager(CosmosDbConfig cosmosConfig, DataGenConfig dataGenConfig) {
        //Example to set cosmos Settings
        String cosmosApi = cosmosConfig.serviceEndpoint;
        String cosmosMasterKey = cosmosConfig.masterKey;
        String cosmosDbName = cosmosConfig.dbName;
        String cosmosTablePrefix = dataGenConfig.collectionPrefix;
        int maxPageSize = -1; //unlimited page size
        int maxPoolSize = 10000; //unlimited page size
        ConnectionMode mode = ConnectionMode.Gateway;
        ConsistencyLevel level = ConsistencyLevel.Eventual; //todo revisit
        int refreshCycleMillis = 5000;
        int refreshTimeoutMillis = 20000;
        CosmosSettings cosmosSettings = new CosmosSettings(
                cosmosApi, cosmosMasterKey, cosmosDbName, cosmosTablePrefix, -1, maxPoolSize,
                mode, level,10,9,20,10000, 60,
                refreshCycleMillis, refreshTimeoutMillis);

        //Example to set GraphManager Settings
        int ingestRetries = 15;
        int persistRetries = 15;
        boolean lockingEnabled = false;
        int concurrentThreads = 1;
        int minRetryWaitTimeMillis = 5;  //increase as needed for cosmos
        int maxRetryWaitTimeMillis = 10; //increase as needed for cosmos
        Enumeration.Value serDeType = SerDeType.PROTOBUF_COSMOS();
        int maxNodeCount = 200;
        int lookupBatchMaxTimeoutMillis = 10000;
        int lookupBatchAttemptTimeoutMillis = 10000;
        int lookupSingleMaxTimeoutMillis = 10000;
        int lookupSingleAttemptTimeoutMillis = 10000;
        boolean cancelOnTimeoutFlag = false;


        GraphManagerSettings graphManagerSettings =
                GraphManagerSettings.apply(
                        lockingEnabled,
                        ingestRetries,
                        persistRetries,
                        concurrentThreads,
                        minRetryWaitTimeMillis,
                        maxRetryWaitTimeMillis,
                        serDeType,
                        maxNodeCount,
                        scala.Option.apply(null),
                        lookupBatchMaxTimeoutMillis,
                        lookupBatchAttemptTimeoutMillis,
                        lookupSingleMaxTimeoutMillis,
                        lookupSingleAttemptTimeoutMillis,
                        cancelOnTimeoutFlag
                );

        //Example in setting Metrics
        MetricSettings metricSettings = new MetricSettings("some.metrix.prefix", new MetricRegistry(), MetricsEnum.DROPWIZARD_METRICS$.MODULE$);



        /**
         * Call GraphManagerFactory to create instance
         *   - In this example we are creating an inmemory store for testing purposes
         *   - cosmosSettings will be ignored in this case
         */
        GraphManagerNew gManager = GraphManagerFactoryNew.apply(
                GraphManagerEnum.SINGULARITY_COSMOS$.MODULE$,
                Option.apply(cosmosSettings),
                Option.apply(graphManagerSettings),
                Option.apply(metricSettings)
        );

        return gManager;
    }
}
