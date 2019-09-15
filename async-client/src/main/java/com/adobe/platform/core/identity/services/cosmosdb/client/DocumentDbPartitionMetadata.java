package com.adobe.platform.core.identity.services.cosmosdb.client;

import java.util.*;
import java.util.stream.Collectors;

import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.internal.routing.CollectionRoutingMap;
import com.microsoft.azure.cosmosdb.internal.routing.IServerIdentity;
import com.microsoft.azure.cosmosdb.internal.routing.InMemoryCollectionRoutingMap;
import com.microsoft.azure.cosmosdb.internal.routing.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
  * Helper class to retrieve partition metadata for a given collection. This is used for grouping IDs by partition in
  * batch queries
  */
class DocumentDbPartitionMetadata {
  Logger logger = LoggerFactory.getLogger(DocumentDbPartitionMetadata.class.getSimpleName());

  private String collectionLink;
  private CollectionRoutingMap collectionRoutingMap;
  private List<String> partitionKeyRangeIds;
  private PartitionKeyDefinition partitionKeyDefinition;

  public DocumentDbPartitionMetadata(AsyncDocumentClient client, String collectionLink){
    this.collectionLink = collectionLink;
    reloadMetadata(client);
  }

  //init
  public void reloadMetadata(AsyncDocumentClient client) {
    ResourceResponse<DocumentCollection> response =
            client.readCollection(this.collectionLink, new RequestOptions()).toBlocking().single();
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
      String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).getId();

      partitionIdMap.putIfAbsent(partitionRangeId, new HashSet<>(ids.size() / partitionKeyRangeIds.size()));
      partitionIdMap.get(partitionRangeId).add(id);
    }

    return partitionIdMap;
  }

  private CollectionRoutingMap getCollectionRoutingMap(AsyncDocumentClient client,String collectionLink){
    FeedResponse<PartitionKeyRange> partitionKeyRanges=
      client.readPartitionKeyRanges(collectionLink, new FeedOptions()).toBlocking().single();

    List<ImmutablePair<PartitionKeyRange, IServerIdentity>> ranges = partitionKeyRanges
      .getResults().stream()
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
            .map(PartitionKeyRange::getId)
            .collect(Collectors.toList());
  }
}