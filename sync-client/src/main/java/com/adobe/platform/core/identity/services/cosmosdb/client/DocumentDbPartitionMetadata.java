package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.routing.CollectionRoutingMap;
import com.microsoft.azure.documentdb.internal.routing.InMemoryCollectionRoutingMap;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public final class DocumentDbPartitionMetadata {
    private static final Logger log = LoggerFactory.getLogger(DocumentDbPartitionMetadata.class.getSimpleName());
    private volatile CollectionRoutingMap collectionRoutingMap;
    private volatile List<String> partitionKeyRangeIds;
    private volatile PartitionKeyDefinition partitionKeyDefinition;
    private final String collectionLink;

    /**
     * @param client
     * @param collectionLink
     * @throws DocumentClientException
     */
    public DocumentDbPartitionMetadata(DocumentClient client, String collectionLink) throws DocumentClientException {
        // Get partition key definition from CosmosDB
        this.collectionLink = collectionLink;
        loadPartitionMetaData(client);
    }

    /**
     * refresh partition meta data by creating a new routing map
     *
     * @param client
     */
    public void loadPartitionMetaData(DocumentClient client) throws DocumentClientException {
        partitionKeyDefinition = client.readCollection(collectionLink, new RequestOptions()).getResource()
                .getPartitionKey();
        collectionRoutingMap = getCollectionRoutingMap(client, collectionLink);
        //TODO: Change the log level to debug
        partitionKeyRangeIds = getCollectionPartitionKeyRangeIds();
    }

    private static CollectionRoutingMap getCollectionRoutingMap(DocumentClient client, String collectionLink) {
        List<ImmutablePair<PartitionKeyRange, Boolean>> ranges = new ArrayList<>();

        List<PartitionKeyRange> partitionKeyRanges = client.readPartitionKeyRanges(collectionLink, (FeedOptions) null)
                .getQueryIterable().toList();

        for (PartitionKeyRange range : partitionKeyRanges) {
            ranges.add(new ImmutablePair<>(range, true));
        }

        CollectionRoutingMap routingMap = null;
        try {
            routingMap = InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(ranges,
                    StringUtils.EMPTY);
        } catch (Exception e) {
            log.error("Exception when creating routing map, message={}, cause={}", e.getMessage(), e.getCause());
        }

        if (routingMap == null) {
            throw new IllegalStateException("Cannot create complete routing map.");
        }

        return routingMap;
    }


    /**
     * @return A list of the partition key ids for the current collection.
     */
    private List<String> getCollectionPartitionKeyRangeIds() {
        Range<String> fullRange = new Range<>(PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey, true, false);

        Collection<PartitionKeyRange> partitionKeyRanges = collectionRoutingMap.getOverlappingRanges(fullRange);

        List<String> rangeIds = partitionKeyRanges.stream().map(partitionKeyRange -> partitionKeyRange.getId()).collect(Collectors.toList());

        log.debug("Got Partition key range ids of size={}", rangeIds.size());

        return rangeIds;
    }

    /**
     * Given a Collection of document ids, returns a map of partition ids to document ids, for documents that fall
     * into each respective partition.
     *
     * @param ids The list of document ids to separate by partition.
     * @return A map of partition ids to document ids.
     */
    public HashMap<String, Set<String>> getIdsByPartition(Collection<String> ids) {
        HashMap<String, Set<String>> partitionIdMap = new HashMap<>();

        for (String id : ids) {
            PartitionKeyInternal partitionKeyValue = PartitionKeyInternal
                    .fromObjectArray(Collections.singletonList(id), true);

            String effectivePartitionKey = partitionKeyValue
                    .getEffectivePartitionKeyString(partitionKeyDefinition, true);

            String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey)
                    .getId();

            partitionIdMap.putIfAbsent(partitionRangeId, new HashSet<>(ids.size() / partitionKeyRangeIds.size()));
            partitionIdMap.get(partitionRangeId).add(id);
        }

        return partitionIdMap;
    }

    public String getCollectionLink() {
        return collectionLink;
    }
}