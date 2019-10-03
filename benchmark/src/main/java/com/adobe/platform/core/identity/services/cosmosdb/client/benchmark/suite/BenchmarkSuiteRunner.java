package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.suite;

import com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util.BenchmarkConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util.SecondaryResultRecorderProfiler;
import org.apache.commons.lang3.tuple.Pair;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class BenchmarkSuiteRunner {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkSuiteRunner.class.getSimpleName());
    private static final String COSMOS_CLIENT_TYPE_PARAM = "cosmosClientType";

    public static void main(String[] args) throws Exception {
        BenchmarkConfig cfg = new BenchmarkConfig();
        BenchmarkSuiteRunner runner = new BenchmarkSuiteRunner();

        BenchmarkResultsHolder results = runner.runBenchmarks(cfg);

        logger.info("Benchmarks complete.\nBenchmarks Results ..\n" + results.getResultsString() + "\n");

        System.exit(0);
    }

    private BenchmarkResultsHolder runBenchmarks(BenchmarkConfig cfg) throws RunnerException, CommandLineOptionException {
        logger.info("Using the following params to run the benchmark -- {}", cfg.getRawConfigString());
        String dateStr = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss").format(new Date());

        CommandLineOptions cliOptions = new CommandLineOptions(cfg.jmhArgs.split(" "));
        BenchmarkResultsHolder resultHolder = new BenchmarkResultsHolder();

        String ext = cliOptions.getResultFormat().get().toString().toLowerCase();

        cfg.runList.stream().forEach(run -> {
            run.threads.stream().forEach(threadCount -> {
                //Measure both throughput and latency
                Stream.of(Pair.of(Mode.Throughput, TimeUnit.SECONDS),
                          Pair.of(Mode.SampleTime, TimeUnit.MILLISECONDS))
                    .forEach(modeTimeUnitPair -> {
                        Options opt = new OptionsBuilder()
                                .parent(cliOptions)
                                .addProfiler(SecondaryResultRecorderProfiler.class)
                                .include(run.regex)
                                .mode(modeTimeUnitPair.getLeft())
                                .timeUnit(modeTimeUnitPair.getRight())
                                .threads(threadCount)
                                .jvmArgs(cfg.jvmArgs)
                                .result(cfg.resultsPath + run.name + "_t_" + threadCount + "." + dateStr + "." + ext)
                                .param(COSMOS_CLIENT_TYPE_PARAM, run.cosmosClientType.toString())
                                .build();

                        try {
                            Collection<RunResult> runResults = new Runner(opt).run();
                            assert (!runResults.isEmpty());
                            resultHolder.recordResults(run, threadCount, runResults);
                            // replace whole file with the full summary
                            writeResultsToCsvFile(cfg.summaryCsvFile, resultHolder);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            });
        });
        return resultHolder;
    }

    public void writeResultsToCsvFile(String filePath, BenchmarkResultsHolder results) throws IOException {
        //Get the file reference
        Path path = Paths.get(filePath);

        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            writer.write(results.getHeaderLine() + "\n");
            writer.write(results.getResultsString());
        }
    }
}
