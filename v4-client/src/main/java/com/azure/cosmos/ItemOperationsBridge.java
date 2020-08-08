package com.azure.cosmos;

import com.azure.cosmos.implementation.apachecommons.lang.tuple.Pair;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import reactor.core.publisher.Mono;

import java.util.List;

public class ItemOperationsBridge {


    public static <T> Mono<FeedResponse<T>> readManyAsync(CosmosAsyncContainer container,
                                                   List<Pair<String, PartitionKey>> itemKeyList,
                                                   CosmosQueryRequestOptions options,
                                                   Class<T> klass) {
        return container.getDatabase()
                .getDocClientWrapper()
                .readMany(itemKeyList, container.getLink(), options, klass);
    }
}
