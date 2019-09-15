package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.List;
import java.util.stream.Collectors;

public class BenchmarkConfig {
    private static final String CONFIG_PREFIX = "benchmark.jmh";

    public static final class Run {
        public String name;
        public String regex;
        public List<Integer> threads;

        public Run(String name, String regex, List<Integer> threads) {
            this.name = name;
            this.regex = regex;
            this.threads = threads;
        }

        @Override
        public String toString() {
            return regex + threads.stream().map(String::valueOf).collect(Collectors.joining());
        }
    }

    public String jvmArgs;
    public String jmhArgs;
    public List<Run> runList;
    public String resultsPath;
    public String summaryCsvFile;

    public BenchmarkConfig() {
        this(ConfigFactory.load());
    }

    public BenchmarkConfig(Config config) {
        this.jvmArgs = config.getString(CONFIG_PREFIX + ".params.jvmArgs");
        this.jmhArgs = config.getString(CONFIG_PREFIX + ".params.jmhArgs");
        this.resultsPath = config.getString(CONFIG_PREFIX + ".params.resultsPath");
        this.summaryCsvFile = config.getString(CONFIG_PREFIX + ".params.summaryCsvFile");

        this.runList = config.getConfigList(CONFIG_PREFIX + ".runList").stream()
                .map(c -> new Run(c.getString("name"), c.getString("regex"), c.getIntList("threads")))
                .collect(Collectors.toList());
    }

    public String getRawConfigString(){
        return ConfigFactory.load().getConfig(CONFIG_PREFIX).toString();
    }
}