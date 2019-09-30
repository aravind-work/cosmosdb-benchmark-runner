package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark;

import com.adobe.platform.core.identity.services.cosmosdb.client.AsyncCosmosDbClient;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleResponse;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;
import com.google.common.util.concurrent.AtomicDouble;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import org.openjdk.jmh.results.AggregationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util.SecondaryResultRecorderProfiler.addCounterResult;

public class AbstractBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBenchmark.class.getSimpleName());
    public static final String ERROR_COUNT_RESULT_NAME = "errorCount";
    public static final String RU_TOTAL = "totalRu";

    private AtomicDouble ruTotal = new com.google.common.util.concurrent.AtomicDouble(0);
    protected AsyncCosmosDbClient client;

    public void commonSetup(CosmosDbConfig cosmosConfig){
        client = new AsyncCosmosDbClient(cosmosConfig);
    }

    protected <T> T performWorkload(ThrowingSupplier<T> workload) {
        try {
            T result =  workload.get();

            if(result instanceof SimpleResponse){
                SimpleResponse response = (SimpleResponse) result;
                double ru = response.getRuUsed();
                ruTotal.addAndGet(ru);
            }

            return result;
        } catch (Throwable th) {
            addCounterResult(ERROR_COUNT_RESULT_NAME, 1, "ops", AggregationPolicy.SUM);
            logger.error("{} Exception in benchmark method. Msg = {}, Cause = {}", th.getClass().getSimpleName(), th.getMessage(), th.getCause() == null ? "null" : th.getCause().getMessage());
            return null;
        }
    }

    public void commonTearDown(){
        logger.info("Benchmarking tear down in progress ...");
        addCounterResult(RU_TOTAL, ruTotal.longValue(), "ru/s", AggregationPolicy.SUM);

        try {Thread.sleep(5000);} catch(InterruptedException e){}
        client.close();
        client.getDocumentClient().close(); //is this needed?
        try {Thread.sleep(10000);} catch(InterruptedException e){}

        logger.info("Benchmarking tear down complete.");
    }
}
