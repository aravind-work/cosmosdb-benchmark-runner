package com.adobe.platform.core.identity.services.datagenerator;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * Expecting type-safe config file in the following format
 *  benchmark {
 *      dataGenerator {
 *
 *       reCreateCollections = false
 *       collectionRusOnCreate = 1600000
 *       collectionRusPostCreate = 133500
 *       tablePrefix = ""
 *
 *       generateTestData = false
 *       testGraphCount = 10000
 *       nodesPerGraph = 6
 *     }
 *  }
 *
 */
public class DataGenConfig {
    public static final String P_KEY = "id";

    public boolean reCreateCollections;
    public int collectionRusOnCreate;
    public int collectionRusPostCreate;
    public String createWithConsistencyLevel;
    public String tablePrefix;

    public boolean generateTestData;
    public int testGraphCount;
    public int nodesPerGraph;
    public boolean performDocCountCheck;
    public String generatorType;

    private static final String CONFIG_PREFIX = "benchmark.dataGenerator.";

    public DataGenConfig(){
        this(ConfigFactory.load());
    }

    public DataGenConfig(Config config){
        this.collectionRusOnCreate = config.getInt(CONFIG_PREFIX + "collectionRusOnCreate");
        this.collectionRusPostCreate = config.getInt(CONFIG_PREFIX + "collectionRusPostCreate");
        this.createWithConsistencyLevel = config.getString(CONFIG_PREFIX + "createWithConsistencyLevel");
        this.tablePrefix = config.getString(CONFIG_PREFIX + "tablePrefix");

        this.reCreateCollections = config.getBoolean(CONFIG_PREFIX + "reCreateCollections");
        this.generateTestData = config.getBoolean(CONFIG_PREFIX + "generateTestData");
        this.testGraphCount = config.getInt(CONFIG_PREFIX + "testGraphCount");
        this.nodesPerGraph = config.getInt(CONFIG_PREFIX + "nodesPerGraph");
        this.performDocCountCheck = config.getBoolean(CONFIG_PREFIX + "performDocCountCheck");
        this.generatorType = config.getString(CONFIG_PREFIX + "generatorType");
    }
}
