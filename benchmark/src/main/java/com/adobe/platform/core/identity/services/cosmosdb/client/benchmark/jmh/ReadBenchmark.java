package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.jmh;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbClientType;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleResponse;
import com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.workload.Workload;
import com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.workload.WorkloadType;
import com.adobe.platform.core.identity.services.cosmosdb.util.CosmosDbException;
import com.adobe.platform.core.identity.services.datagenerator.DataGenConfig;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@State(Scope.Benchmark)
public class ReadBenchmark extends AbstractBenchmark{
    private static final Logger logger = LoggerFactory.getLogger(ReadBenchmark.class.getSimpleName());

    private Workload workload;
    private CosmosDbClientType clientType;

    @Param({"sync", "async"})
    String cosmosClientType;

    @Setup(Level.Trial)
    public void setup(){
        super.commonSetup();

        clientType = CosmosDbClientType.valueOf(cosmosClientType.toUpperCase());

        DataGenConfig datagenConfig = new DataGenConfig();
        CosmosDbConfig cosmosConfig = new CosmosDbConfig();

        workload = Workload.getInstance(WorkloadType.TWO_TABLE_SIMPLE, clientType, datagenConfig.collectionPrefix, cosmosConfig);

        workload.workloadSetup();
    }

    @Benchmark
    public SimpleResponse lookupRoutingSingle(){ return performWorkload(workload.lookupRoutingSingle()); }

    @Benchmark
    public SimpleResponse lookupRoutingBatch() {
        return performWorkload(workload.lookupRoutingBatch());
    }

    @Benchmark
    public SimpleResponse lookupTwoTableSingle() {
        return performWorkload(workload.lookupTwoTableSingle());
    }

    @Benchmark
    public SimpleResponse lookupTwoTableBatch() {
        return performWorkload(workload.lookupTwoTableBatch());
    }

    @Benchmark //This is for an experiment only
    public SimpleResponse multiThreadLookupRoutingBatch() { return performWorkload(workload.lookupRoutingBatchMultiThread()); }

    @TearDown
    public void tearDown(){
        super.commonTearDown();
        workload.workloadTeardown();
    }


    // FOR DEBUGGING ONLY !
    public static void main(String args[]) throws CosmosDbException {
        String challengeResponse = "Today's password is swordfish. I understand instantiating Blackholes directly " +
                                    "is dangerous.";
        int iterations = 10000;

        ReadBenchmark bench = new ReadBenchmark();
        bench.cosmosClientType = "sync";
        bench.setup();

        try {
            for (int i = 0; i < iterations; i++) {
                long startTime = System.currentTimeMillis();
                bench.lookupRoutingSingle();
                logger.info("Latency = {} ms", (System.currentTimeMillis() - startTime));
            }
        }
        catch(Throwable th) {
            throw th;
        }
        finally
        {
            bench.tearDown();
        }
    }
}
