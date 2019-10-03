# Benchmark Runner for Cosmos DB 

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
`java -cp benchmark/build/libs/benchmark-1.2-cosmos-2.4.3-SNAPSHOT-shadow.jar com.adobe.platform.core.identity.services.datagenerator.main.DataGenUtil`

## Run benchmarks
`java -cp benchmark/build/libs/benchmark-1.2-cosmos-2.4.3-SNAPSHOT-shadow.jar com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.suite.BenchmarkSuiteRunner | tee benchmark.out`

## Debug in IDE
- Run this main method `com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.jmh.ReadBenchmark.main`
- This will simply exercise readDocument(..)

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
```
OpName, ThreadCount, Throughput(ops/s),  Throughput(+/-), P95(ms), P99(ms), OpCount, ErrorCount, ErrorRate
lookup-single-async-cosmos-v2.6.1, 1, 782.71, NaN, 1.51, 1.83, 557409, 0, 0.00
```
Benchmark hangs for threadCount > 1, the following messages are logged
```
2019-10-03 18:38:16,046       [CosmosEventLoop-5-18] WARN  com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneAndRetryWithRetryPolicy - Received gone exception, will retry, GoneException{error=null, resourceAddress='rntbd://cdb-ms-prod-eastus2-fd14.documents.azure.com:16748/apps/16d6d56e-e73d-4069-98f5-2e677328a7d1/services/1e12e422-b053-47c7-9c03-c5a1b1d73024/partitions/c0ec71ae-5880-40a5
-a9e6-d968b93c7620/replicas/132142763239106974p/', statusCode=410, message=ChannelHandlerContext(RntbdRequestManager#0, [id: 0x3f515e5b, L:/172.19.0.4:57232 - R:cdb-ms-prod-eastus2-fd14.documents.azure.com/104.208.231.8:16748]) closed exceptionally with 3 pending requests, getCauseInfo=[class: class java.lang.IllegalStateException, message: null], responseHeaders={}, requestHeaders={authorization=ty
pe%3Dmaster%26ver%3D1.0%26sig%3DP2mCg7rATURTyPNA7Fr7Ka%2BU8%2F9DqT2sQ0UUbA%2BkjEg%3D, Accept=application/json, x-ms-date=Thu, 03 Oct 2019 18:38:16 GMT, x-ms-documentdb-collection-rid=j7M8ANVKp0o=, x-ms-client-retry-attempt-count=0, x-ms-documentdb-partitionkey=["2f60d4c0-d7f0-47d9-983c-b6e23d2b20d3"], x-ms-remaining-time-in-ms-on-client=60000, x-ms-consistency-level=Eventual}}
2019-10-03 18:38:16,046       [CosmosEventLoop-5-18] WARN  com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneAndRetryWithRetryPolicy - Received gone exception, will retry, GoneException{error=null, resourceAddress='rntbd://cdb-ms-prod-eastus2-fd14.documents.azure.com:16748/apps/16d6d56e-e73d-4069-98f5-2e677328a7d1/services/1e12e422-b053-47c7-9c03-c5a1b1d73024/partitions/c0ec71ae-5880-40a5
-a9e6-d968b93c7620/replicas/132142763239106974p/', statusCode=410, message=ChannelHandlerContext(RntbdRequestManager#0, [id: 0x3f515e5b, L:/172.19.0.4:57232 - R:cdb-ms-prod-eastus2-fd14.documents.azure.com/104.208.231.8:16748]) closed exceptionally with 3 pending requests, getCauseInfo=[class: class java.lang.IllegalStateException, message: null], responseHeaders={}, requestHeaders={authorization=ty
pe%3Dmaster%26ver%3D1.0%26sig%3D3Da8P%2FHJVWUqrlkeP8jW4mae%2FKf65eqt9Zo9%2BmJKBk8%3D, Accept=application/json, x-ms-date=Thu, 03 Oct 2019 18:38:16 GMT, x-ms-documentdb-collection-rid=j7M8ANVKp0o=, x-ms-client-retry-attempt-count=0, x-ms-documentdb-partitionkey=["f1e76dd8-a971-4baa-8f13-7f2c864d1fb0"], x-ms-remaining-time-in-ms-on-client=60000, x-ms-consistency-level=Eventual}}
2019-10-03 18:38:16,409       [CosmosEventLoop-5-2] ERROR AsyncCosmosDbClient - Error in getDocument()
RequestTimeoutException{error=null, resourceAddress='rntbd://cdb-ms-prod-eastus2-fd6.documents.azure.com:14044/apps/3956b2e2-7d8a-4c54-b795-671c83ed6192/services/819269b8-9cf6-467c-804a-d6bdcdb860f6/partitions/f402a164-a189-4a98-a690-47136496a0b2/replicas/132142461299436313s/', statusCode=408, message=Request timeout interval (60,000 ms) elapsed, 
RequestStartTime: "03 Oct 2019 18:37:16.394", RequestEndTime: "03 Oct 2019 18:38:16.408", Duration: 60014 ms, Number of regions attempted: 1
StoreResponseStatistics{requestResponseTime="03 Oct 2019 18:38:16.408", storeResult=storePhysicalAddress: null, lsn: -1, globalCommittedLsn: -1, partitionKeyRangeId: null, isValid: true, statusCode: 408, subStatusCode: 0, isGone: false, isNotFound: false, isInvalidPartition: false, requestCharge: 0.0, itemLSN: -1, sessionToken: null, exception: Request timeout interval (60,000 ms) elapsed, requestRe
sourceType=Document, requestOperationType=Read}
, getCauseInfo=null, responseHeaders={}, requestHeaders={authorization=type%3Dmaster%26ver%3D1.0%26sig%3DipjgCZ%2B%2BkRCr3cdUQr39lyTkQkQ8Oevj0xI%2BOIKOXNE%3D, Accept=application/json, x-ms-date=Thu, 03 Oct 2019 18:37:16 GMT, x-ms-documentdb-collection-rid=j7M8ANVKp0o=, x-ms-client-retry-attempt-count=0, x-ms-documentdb-partitionkey=["919af32a-705a-4077-a6c7-667d23813b03"], x-ms-remaining-time-in-ms-
on-client=60000, x-ms-consistency-level=Eventual}}
        at com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestRecord.expire(RntbdRequestRecord.java:84)
        at io.netty.util.concurrent.AbstractEventExecutor.safeExecute(AbstractEventExecutor.java:163)
        at io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:416)
        at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:515)
        at io.netty.util.concurrent.SingleThreadEventExecutor$5.run(SingleThreadEventExecutor.java:918)
        at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
        at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
        at java.lang.Thread.run(Thread.java:748)
2019-10-03 18:38:16,410       [CosmosEventLoop-5-24] WARN  com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneAndRetryWithRetryPolicy - Received gone exception, will retry, GoneException{error=null, resourceAddress='rntbd://cdb-ms-prod-eastus2-fd14.documents.azure.com:14115/apps/506e03a2-1c37-4e4c-bd59-9369bbf4d4b8/services/df5ca914-7ade-4fbd-b488-2543f84b695d/partitions/1f999bf9-3451-4de7
-accb-72721fda4abb/replicas/132142763257700618s/', statusCode=410, message=ChannelHandlerContext(RntbdRequestManager#0, [id: 0x748bbb93, L:/172.19.0.4:57992 - R:cdb-ms-prod-eastus2-fd14.documents.azure.com/104.208.231.8:14115]) closed exceptionally with 1 pending requests, getCauseInfo=[class: class java.lang.IllegalStateException, message: null], responseHeaders={}, requestHeaders={authorization=ty
pe%3Dmaster%26ver%3D1.0%26sig%3D9Gb05907BrPiFt%2BP39nw2XodyTxLaBocyee8Xuegwwo%3D, Accept=application/json, x-ms-date=Thu, 03 Oct 2019 18:38:16 GMT, x-ms-documentdb-collection-rid=j7M8ANVKp0o=, x-ms-client-retry-attempt-count=0, x-ms-documentdb-partitionkey=["8bd3d587-456f-45eb-9204-fdadc1f4e58d"], x-ms-remaining-time-in-ms-on-client=60000, x-ms-consistency-level=Eventual}}
2019-10-03 18:38:16,410       [com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.jmh.ReadBenchmark.lookupRoutingSingle-jmh-worker-39] ERROR AbstractBenchmark - CosmosDbException Exception in benchmark method. Msg = A cosmosDB exception has occurred!, Cause = Request timeout interval (60,000 ms) elapsed, 
RequestStartTime: "03 Oct 2019 18:37:16.394", RequestEndTime: "03 Oct 2019 18:38:16.408", Duration: 60014 ms, Number of regions attempted: 1
StoreResponseStatistics{requestResponseTime="03 Oct 2019 18:38:16.408", storeResult=storePhysicalAddress: null, lsn: -1, globalCommittedLsn: -1, partitionKeyRangeId: null, isValid: true, statusCode: 408, subStatusCode: 0, isGone: false, isNotFound: false, isInvalidPartition: false, requestCharge: 0.0, itemLSN: -1, sessionToken: null, exception: Request timeout interval (60,000 ms) elapsed, requestRe
sourceType=Document, requestOperationType=Read}

2019-10-03 18:38:17,506       [CosmosEventLoop-5-16] WARN  com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneAndRetryWithRetryPolicy - Received gone exception, will retry, GoneException{error=null, resourceAddress='rntbd://cdb-ms-prod-eastus2-fd6.documents.azure.com:14047/apps/d46d193b-507c-4102-9c2d-41d3997eb75e/services/6fe4eab3-c60a-4d12-ad9d-718b1db9910d/partitions/59e24c12-a30d-481a-
a82f-7dcf6e258b17/replicas/132144188441640655s/', statusCode=410, message=ChannelHandlerContext(RntbdRequestManager#0, [id: 0xd5a031f1, L:/172.19.0.4:58956 - R:cdb-ms-prod-eastus2-fd6.documents.azure.com/104.208.231.0:14047]) closed exceptionally with 2 pending requests, getCauseInfo=[class: class java.lang.IllegalStateException, message: null], responseHeaders={}, requestHeaders={authorization=type
%3Dmaster%26ver%3D1.0%26sig%3DrbcKmQsWk48TvvXXaqY7uvWz2BT3ReHuI%2B35MeZpq%2F0%3D, Accept=application/json, x-ms-date=Thu, 03 Oct 2019 18:38:17 GMT, x-ms-documentdb-collection-rid=j7M8ANVKp0o=, x-ms-client-retry-attempt-count=0, x-ms-documentdb-partitionkey=["330d5cfd-923e-4657-b253-713dd7695a0c"], x-ms-remaining-time-in-ms-on-client=60000, x-ms-consistency-level=Eventual}}

```