package com.azure.cosmos;

import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Mono;

import java.util.List;

public class ItemOperationsBridge {


    public static <T> Mono<FeedResponse<T>> readManyAsync(CosmosAsyncContainer container,
                                                   List<Pair<String, PartitionKey>> itemKeyList,
                                                   FeedOptions options,
                                                   Class<T> klass) {
        return container.getDatabase()
                .getDocClientWrapper()
                .readMany(itemKeyList, container.getLink(), options, klass);
    }
}
