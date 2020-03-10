package com.adobe.platform.core.identity.services.cosmosdb.client;

import java.util.*;
import java.util.stream.Collectors;


import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.FeedOptions;
import com.azure.data.cosmos.FeedResponse;
import com.azure.data.cosmos.PartitionKeyDefinition;
import com.azure.data.cosmos.internal.*;
import com.azure.data.cosmos.internal.routing.*;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
  * Helper class to retrieve partition metadata for a given collection. This is used for grouping IDs by partition in
  * batch queries
  */
class V3DocumentDbPartitionMetadata {
  Logger logger = LoggerFactory.getLogger(V3DocumentDbPartitionMetadata.class.getSimpleName());

  private String collectionLink;
  private CollectionRoutingMap collectionRoutingMap;
  private List<String> partitionKeyRangeIds;
  private PartitionKeyDefinition partitionKeyDefinition;

  public V3DocumentDbPartitionMetadata(CosmosClient client, String collectionLink){
    this.collectionLink = collectionLink;

    AsyncDocumentClient documentClient = get(AsyncDocumentClient.class, client, "asyncDocumentClient");
    reloadMetadata(documentClient);
  }

  private static <T> T get(Class<T> klass, Object object, String fieldName) {
    try {
      return (T) FieldUtils.readField(object, fieldName, true);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  //init
  public void reloadMetadata(AsyncDocumentClient client) {
    ResourceResponse<DocumentCollection> response =
            client.readCollection(this.collectionLink, new RequestOptions()).blockFirst();
    this.partitionKeyDefinition = response.getResource().getPartitionKey();
    this.collectionRoutingMap = this.getCollectionRoutingMap(client, this.collectionLink);
    this.partitionKeyRangeIds = this.getCollectionPartitionKeyRangeIds(this.collectionRoutingMap);
  }

  /**
   * Group IDs by partition-range-id
   *
   * @param ids - A list of IDs. Each ID identifies a document in the collection
   * @return - A Map each Keys is a PartitionRangeId and the value is a Set of IDs that fall into this partition range.
   */
  public Map<String, Set<String>> groupIdsByPartitionRangeId(List<String> ids) {

    Map<String, Set<String>> partitionIdMap = new HashMap<>();
    Iterator<String> iter = ids.iterator();

    while (iter.hasNext()) {
      String id = iter.next();

      PartitionKeyInternal partitionKeyValue = PartitionKeyInternal.fromObjectArray(Collections.singletonList(id).toArray(), true);
      String effectivePartitionKey = PartitionKeyInternalHelper.getEffectivePartitionKeyString(partitionKeyValue, partitionKeyDefinition);
      String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).id();

      partitionIdMap.putIfAbsent(partitionRangeId, new HashSet<>(ids.size() / partitionKeyRangeIds.size()));
      partitionIdMap.get(partitionRangeId).add(id);
    }

    return partitionIdMap;
  }

  private CollectionRoutingMap getCollectionRoutingMap(AsyncDocumentClient client,String collectionLink){
    FeedResponse<PartitionKeyRange> partitionKeyRanges=
      client.readPartitionKeyRanges(collectionLink, new FeedOptions()).blockFirst();

    List<ImmutablePair<PartitionKeyRange, IServerIdentity>> ranges = partitionKeyRanges
      .results().stream()
      .map(r -> new ImmutablePair<PartitionKeyRange, IServerIdentity>(r, new IServerIdentity() {}))
            .collect(Collectors.toList());

    try {
      return InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(ranges, collectionLink);
    } catch (Exception ex){
        throw new RuntimeException("Cannot create complete routing map for collectionLink " + collectionLink, ex);
    }
  }

  private List<String> getCollectionPartitionKeyRangeIds(CollectionRoutingMap collectionRoutingMap) {
    Range<String> fullRange = new Range<>(PartitionKeyInternalHelper.MinimumInclusiveEffectivePartitionKey,
      PartitionKeyInternalHelper.MaximumExclusiveEffectivePartitionKey, true, false);

    return collectionRoutingMap.getOverlappingRanges(fullRange).stream()
            .map(PartitionKeyRange::id)
            .collect(Collectors.toList());
  }
}