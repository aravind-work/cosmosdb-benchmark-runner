# Adobe Benchmark Runner for Cosmos DB 

This repo is used for benchmarking Cosmos DB SDK against various workloads

## Data Model
The use-case is the following 
- Persist the user provided *sets* (in practice this is a graph, but a set models the same workload in a simpler way) in the DB e.g. [ G1{A,B,C}, G2{X,Y}, G3{P,Q,R,S}]
- Support the query, provided any one of the set members return the original set. e.g. Given Q, return {P,Q,R,S}

To support this use case, we model the data using two collections
1. A graph collection that stores each Set with the key as the Set name (or GraphId). e.g. Key = "G1", Value = "A,B,C"
2. A routing collection that point each member to the Set name (or GraphId). e.g.for set G1, we have the following 3 documents - [Key = "A", Value = "G1"]  [Key = "B", Value = "G1"]  [Key = "C", Value = "G1"]


## Workloads
JMH is used as the benchmarking harness (https://openjdk.java.net/projects/code-tools/jmh/) \
See `com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.jmh.ReadBenchmark` for the JMH annotated class.

The following are the workloads that have been modelled 
1. lookupRoutingSingle - Lookup a single document in the routing collection by calling readDocument(..)
2. lookupRoutingBatch  - Lookup a batch of 1000 documents in the routing collection. This is done by grouping the 1000 keys by the partitionRangeId that they fall into and issuing 1 query per partition.
3. lookupTwoTableSingle - Given a key, do a lookup in Routing collection by calling readDocument(..), use the graph-id present in the retrieved document to fetch the corresponding graph from the Graph collection.
4. lookupTwoTableBatch - Same as #3, but for batches of 1000 keys. This translates into a call to lookupRoutingBatch, followed by a call to lookupGraphBatch.

## Building runnable jar
+ Provide correct CosmosDB Account/DB details. Update client-common/src/main/resources/reference.conf
+ Configure the benchmark runner. Update benchmark/src/main/resources/reference.conf. Description of the config block follows
```benchmark {
     jmh {
       params {
         jvmArgs = "-Xmx8G"
         jmhArgs = "-f1 -i 1 -r 1 -w 1 -wi 1 -t 1 -rf json" // see https://github.com/guozheng/jmh-tutorial/blob/master/README.md#jmh-command-line-options
         resultsPath = "/tmp/"                              // The detailed results file for each run goes here
         summaryCsvFile = "/tmp/benchmark-results.csv"      // A simple consolidated summary of all the runs go here
       }
       runList = [                                          // Specify n number of benchmark runs
         {
           name = "lookup-single-sync"                      // Name for this run
           regex = "ReadBenchmark.lookupRoutingSingle"      // Regex to use to pickup benchmark methods
           threads = [1]//[1,50,100, 125]                   // Number of threads to use for benchmark. We do a separate benchmark for each thread in the array.
           clientType = "sync"                              // Specify whether to use cosmos sync or async SDK.
         },
         {
           name = "lookup-single-async"
           regex = "ReadBenchmark.lookupRoutingSingle"
           threads = [1]//[1,50,100,125,250]//[1,10,50,100,500,750,1000]
           clientType = "async"
         }
       ]
     }
   }
```

+ To build runnable jar run
`./gradlew shadowJar -PcosmosAsyncVersion=2.4.3`
+ Copy to target machine
`scp benchmark/build/libs/benchmark-1.2-cosmos-2.4.3-SNAPSHOT-shadow.jar arsriram@52.184.191.216:~/`
+ Modify config after building shadow jar (optional)
    - You can modify the reference.conf file inside the runnable jar to configure DB account, DB name, MasterKey
    - `vim benchmark-1.2-cosmos-2.4.3-SNAPSHOT-shadow.jar`
    - Hit enter on reference.conf to enter file
    - Look for the following block and modify as needed. :wq to save file and :q! to exit Vim zip browser.

## Generate test collection and data
`java -cp ./benchmark-1.2-cosmos-2.4.3-SNAPSHOT-shadow.jar com.adobe.platform.core.identity.services.datagenerator.main.DataGenUtil`

## Run benchmarks
`java -cp ./benchmark-1.2-cosmos-2.4.3-SNAPSHOT-shadow.jar com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.suite.BenchmarkSuiteRunner | tee benchmark.out`