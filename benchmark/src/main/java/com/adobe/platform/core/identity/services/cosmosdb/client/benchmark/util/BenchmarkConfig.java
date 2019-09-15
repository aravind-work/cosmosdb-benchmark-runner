package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.util;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbClientType;
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
        public CosmosDbClientType cosmosClientType;

        public Run(String name, String regex, List<Integer> threads, CosmosDbClientType cosmosClientType ) {
            this.name = name;
            this.regex = regex;
            this.threads = threads;
            this.cosmosClientType = cosmosClientType;
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
                .map(c -> {
                    CosmosDbClientType clientType = CosmosDbClientType.valueOf(c.getString("clientType").toUpperCase());
                    return new Run(c.getString("name"), c.getString("regex"), c.getIntList("threads"), clientType);
                })
                .collect(Collectors.toList());
    }

    public String getRawConfigString(){
        return ConfigFactory.load().getConfig(CONFIG_PREFIX).toString();
    }
}