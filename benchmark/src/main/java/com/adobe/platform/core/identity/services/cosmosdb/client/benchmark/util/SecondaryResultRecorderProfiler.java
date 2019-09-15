package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;

import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Record misc secondary result metrics.
 */
public class SecondaryResultRecorderProfiler implements InternalProfiler {

    public static final String SECONDARY_RESULT_PREFIX = "+misc.";

    static final Map<String, CounterResult> counters = new ConcurrentHashMap<>();
    static final Map<String, Result<?>> results = new ConcurrentHashMap<>();

    /**
     * Insert the counter value as secondary result. If a value is already inserted the
     * counter value is added to the existing one. This can be used to collect and sum up
     * results from different threads.
     */
    public static void addCounterResult(String key, long _counter, String _unit, AggregationPolicy _aggregationPolicy) {
        CounterResult r = counters.computeIfAbsent(key, any -> new CounterResult());
        r.aggregationPolicy = _aggregationPolicy;
        r.unit = _unit;
        r.counter.addAndGet(_counter);
        r.key = key;
    }

    public static long getCounterResult(String key) {
        return counters.getOrDefault(key, new CounterResult()).counter.get();
    }

    /**
     * Insert the counter value as secondary result. An existing counter value is replaced.
     */
    public static void setResult(String key, double _result, String _unit, AggregationPolicy _aggregationPolicy) {
        setResult(new ScalarResult(SECONDARY_RESULT_PREFIX + key, _result, _unit, _aggregationPolicy));
    }

    /**
     * Add result to the JMH result data. If called multiple times with the same label only the last one will be added.
     */
    public static void setResult(Result r) {
        results.put(r.getLabel(), r);
    }

    @Override
    public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
        counters.clear();
    }

    @Override
    public Collection<? extends Result> afterIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams, final IterationResult result) {
        List<Result<?>> all = new ArrayList<>();
        counters.values().stream()
                .map(e ->
                        new ScalarResult(SECONDARY_RESULT_PREFIX + e.key, (double) e.counter.get(), e.unit, e.aggregationPolicy))
                .sequential().forEach(e -> all.add(e));
        all.addAll(results.values());
        return all;
    }

    @Override
    public String getDescription() {
        return "Adds additional results gathered by the benchmark as secondary results.";
    }

    static class CounterResult {
        String key;
        String unit;
        AggregationPolicy aggregationPolicy;
        AtomicLong counter = new AtomicLong();
    }

}