package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.workload;

import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbClientType;
import com.adobe.platform.core.identity.services.cosmosdb.client.CosmosDbConfig;
import com.adobe.platform.core.identity.services.cosmosdb.client.SimpleResponse;
import com.adobe.platform.core.identity.services.cosmosdb.util.ThrowingSupplier;

public interface Workload {

    void workloadSetup();
    void workloadTeardown();

    ThrowingSupplier<SimpleResponse> lookupRoutingSingle();
    ThrowingSupplier<SimpleResponse> lookupRoutingBatch();
    ThrowingSupplier<SimpleResponse> lookupRoutingBatchMultiThread();

    ThrowingSupplier<SimpleResponse> lookupTwoTableSingle();
    ThrowingSupplier<SimpleResponse> lookupTwoTableBatch();

    // Factory method
    static Workload getInstance(WorkloadType workloadType, CosmosDbClientType clientType, String collectionPrefix, CosmosDbConfig cosmosConfig){
        if(workloadType.equals(workloadType.TWO_TABLE_SIMPLE)){
            return new TwoTableWorkload(cosmosConfig, collectionPrefix, clientType);
        } else {
            throw new UnsupportedOperationException("Workload type " + workloadType.toString() + " not supported");
        }
    }
}
