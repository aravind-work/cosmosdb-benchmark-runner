package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.suite;

import com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util.BenchmarkConfig.Run;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingConsumer;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import static com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.AbstractBenchmark.ERROR_COUNT_RESULT_NAME;
import static com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util.SecondaryResultRecorderProfiler.SECONDARY_RESULT_PREFIX;

public class BenchmarkResultsHolder {
    private final static Logger logger = LoggerFactory.getLogger(BenchmarkResultsHolder.class.getSimpleName());

    public TreeMap<String, FinalResult> resultsMap = new TreeMap<>();

    public static final class FinalResult {
        String operationName;
        int threadCount;
        float throughput;
        float throughputError;

        double p95;
        double p99;

        long opCount = 0L;
        long errorCount = 0L;

        public FinalResult(String operationName){
            this.operationName = operationName;
        }

        public String toCsv(){
            return String.format("%s, %d, %.2f, %.2f, %.2f, %.2f, %d, %d, %.2f\n", operationName, threadCount, throughput, throughputError, p95, p99, opCount, errorCount, (float)errorCount/opCount);
        }
    }

    public String writeHeaderLine(){
        return String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s\n", "OpName", "ThreadCount", "Throughput(ops/s)", " Throughput(+/-)", "P95(ms)", "P99(ms)", "OpCount", "ErrorCount", "ErrorRate");
    }

    public void recordResults(Run run, int threadCount, Collection<RunResult> runResults) {
        runResults.stream()
                .flatMap(runResult -> runResult.getBenchmarkResults().stream())
                .forEach(r-> {
                    Result result = r.getPrimaryResult();

                    // used as key in tree map
                    String nameWithThreadCount = String.format("%s_%05d",run.name, threadCount);
                    resultsMap.putIfAbsent(nameWithThreadCount, new FinalResult(run.name));
                    FinalResult bResult = resultsMap.get(nameWithThreadCount);

                    bResult.threadCount = threadCount;
                    if (r.getParams().getMode().equals(Mode.Throughput)) {
                        bResult.throughput = (float) result.getScore();
                        bResult.throughputError = (float) result.getScoreError();
                    } else if (r.getParams().getMode().equals(Mode.SampleTime)) {
                        Statistics stats = r.getPrimaryResult().getStatistics();

                        bResult.p95 = stats.getPercentile(95);
                        bResult.p99 = stats.getPercentile(95);
                    }

                    // record secondary results
                    // incrementing to aggregate counts between latency and through iterations
                    Map<String, Result> secondaryResult = r.getSecondaryResults();
                    if(secondaryResult.containsKey(SECONDARY_RESULT_PREFIX + ERROR_COUNT_RESULT_NAME)) {
                        bResult.errorCount += (long) secondaryResult.get(SECONDARY_RESULT_PREFIX + ERROR_COUNT_RESULT_NAME).getScore();
                    }
                    bResult.opCount += r.getMetadata().getWarmupOps() + r.getMetadata().getMeasurementOps();

                });
    }

    public void writeResultsToCsvFile(String filePath) throws IOException {
        //Get the file reference
        Path path = Paths.get(filePath);

        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            writer.write(writeHeaderLine());
            resultsMap.keySet().forEach(
                ThrowingConsumer.wrap(
                    key -> writer.write(resultsMap.get(key).toCsv())));
        }
    }

    public void writeToStdout(){
        resultsMap.keySet().forEach(key -> System.out.println(resultsMap.get(key).toCsv()));
    }
}
