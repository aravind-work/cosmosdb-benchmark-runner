package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.jmh;

import com.adobe.platform.core.identity.services.cosmosdb.client.*;
import com.google.common.util.concurrent.AtomicDouble;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;
import org.openjdk.jmh.results.AggregationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util.SecondaryResultRecorderProfiler.addCounterResult;

public class AbstractBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBenchmark.class.getSimpleName());

    public static final String ERROR_COUNT_RESULT_NAME = "errorCount";
    public static final String RU_TOTAL = "totalRu";

    private AtomicDouble ruTotal = new AtomicDouble(0);

    public void commonSetup() {
        // noop
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
            th.printStackTrace();;
            addCounterResult(ERROR_COUNT_RESULT_NAME, 1, "ops", AggregationPolicy.SUM);
            logger.error("{} Exception in benchmark method. Msg = {}, Cause = {}", th.getClass().getSimpleName(), th.getMessage(), th.getCause() == null ? "null" : th.getCause().getMessage());
            return null;
        }
    }

    public void commonTearDown(){
        addCounterResult(RU_TOTAL, ruTotal.longValue(), "ru/s", AggregationPolicy.SUM);
    }
}
