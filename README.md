# adobe-cosmosdb-benchmark

## Building runnable jar
- To build runnable jar run
`./gradlew shadowJar`
- Copy to target machine
`scp benchmark/build/libs/benchmark-1.0-SNAPSHOT-shadow.jar arsriram@52.184.191.216:~/`

## Generate test data
- To generate collections and test data
`java -cp ./benchmark-1.0-SNAPSHOT-shadow.jar com.adobe.platform.core.identity.services.datagenerator.main.DataGenUtil | tee datagen.out`

## Running benchmarks
- To run benchmarks run
`java -cp ./benchmark-1.0-SNAPSHOT-shadow.jar com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.suite.BenchmarkSuiteRunner | tee benchmark.out`