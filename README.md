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

## Results of benchmark runs
### Single Lookup workload
#### Sync SDK v2.4.0 vs Async SDK v2.4.3
refer to /benchmark/results/2.4.3/lookup-single
```
OpName, ThreadCount, Throughput(ops/s),  Throughput(+/-), P95(ms), P99(ms), OpCount, ErrorCount, ErrorRate

lookup-single-sync, 1, 729.42, NaN, 1.67, 2.00, 510189, 0, 0.00
lookup-single-sync, 50, 40262.35, NaN, 1.47, 2.10, 28706975, 0, 0.00
lookup-single-sync, 100, 69682.23, NaN, 1.86, 4.10, 49662923, 0, 0.00
lookup-single-sync, 125, 81009.16, NaN, 2.53, 6.37, 56384645, 0, 0.00
lookup-single-sync, 250, 89921.72, NaN, 5.41, 17.14, 62845832, 0, 0.00
lookup-single-sync, 300, 89358.84, NaN, 7.25, 24.12, 62005360, 0, 0.00

lookup-single-async, 1, 659.74, NaN, 1.80, 2.11, 471670, 0, 0.00
lookup-single-async, 50, 32395.38, NaN, 1.80, 2.42, 23345061, 0, 0.00
lookup-single-async, 100, 54134.88, NaN, 3.22, 5.18, 37266361, 0, 0.00
lookup-single-async, 125, 54486.74, NaN, 4.45, 7.95, 38483976, 0, 0.00
lookup-single-async, 250, 63342.66, NaN, 13.14, 22.54, 42745108, 0, 0.00
lookup-single-async, 300, 64177.47, NaN, 14.47, 24.74, 45406863, 0, 0.00
lookup-single-async, 400, 62915.07, NaN, 23.66, 37.95, 42994734, 0, 0.00
lookup-single-async, 500, 64044.00, NaN, 29.92, 46.47, 42813558, 0, 0.00
```
#### Async SDK v2.6.1
refer to /benchmark/results/2.6.1/lookup-single
```
todo
```
### Batch Lookup workload
#### Sync SDK v2.4.0 vs Async SDK v2.4.3 
refer to /benchmark/results/2.4.3/lookup-batch/benchmark-results-09-30-2019-cosmos-v2.4.3.csv
```
OpName, ThreadCount, Throughput(ops/s),  Throughput(+/-), P95(ms), P99(ms), OpCount, ErrorCount, ErrorRate

lookup-batch, 1, 30.15, NaN, 41.16, 56.72, 21094, 0, 0.00
lookup-batch, 2, 51.31, NaN, 47.53, 258.21, 36728, 0, 0.00
lookup-batch, 4, 108.01, NaN, 53.67, 66.17, 77079, 0, 0.00
lookup-batch, 8, 148.67, NaN, 78.12, 95.42, 107124, 0, 0.00
lookup-batch, 16, 149.77, NaN, 143.39, 254.80, 106294, 0, 0.00
lookup-batch, 32, 109.37, NaN, 517.47, 561.55, 75882, 0, 0.00
lookup-batch, 64, 73.40, NaN, 1119.88, 1210.06, 51717, 0, 0.00
lookup-batch, 128, 37.81, NaN, 7902.07, 9279.48, 23137, 0, 0.00

multi-thread-lookup-batch, 1, 64.15, NaN, 22.52, 28.40, 46599, 0, 0.00
multi-thread-lookup-batch, 2, 106.13, NaN, 29.36, 39.52, 75556, 0, 0.00
multi-thread-lookup-batch, 4, 137.14, NaN, 43.91, 55.04, 98971, 0, 0.00
multi-thread-lookup-batch, 8, 145.50, NaN, 79.82, 90.70, 106263, 0, 0.00
multi-thread-lookup-batch, 16, 145.81, NaN, 154.66, 176.42, 101407, 0, 0.00
multi-thread-lookup-batch, 32, 132.65, NaN, 299.37, 410.52, 95952, 0, 0.00
multi-thread-lookup-batch, 64, 127.20, NaN, 676.33, 727.71, 88310, 0, 0.00
multi-thread-lookup-batch, 128, 121.05, NaN, 1254.10, 1327.50, 85268, 0, 0.00
```
#### Async SDK v2.6.1
```
todo
```