package com.adobe.platform.core.identity.services.cosmosdb.client.benchmark.workload;


import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public abstract class AbstractWorkload implements Workload {
    private static final Random rnd = new Random();

    static final int BATCH_QUERY_SIZE = 1000;
    static final int MAX_PER_PARTITION_QUERY_BATCH_SIZE = 100;
    static final int MAX_IDS_TO_FETCH_PER_PARTITION = 1000;

    // Helper functions

    public static int selectRandomPartitionRangeId(List<List<String>> idsPerPartition){
        return rnd.nextInt(idsPerPartition.size());
    }

    public static String selectRandomDocId(List<List<String>> idsPerPartition){
        int partitionId = selectRandomPartitionRangeId(idsPerPartition);
        return selectRandomDocId(partitionId, idsPerPartition);
    }

    public static String selectRandomDocId(int partitionId, List<List<String>> idsPerPartition){
        List<String> keysInThisPartition = idsPerPartition.get(partitionId);
        int index = rnd.nextInt(keysInThisPartition.size());
        return keysInThisPartition.get(index);
    }

    public static Set<String> selectDocIdsAcrossPartitions(int keyCount, List<List<String>> idsPerPartition){
        int docCount = idsPerPartition.stream().mapToInt(List::size).sum();
        if(keyCount > docCount){
            throw new RuntimeException("Number of documents requested documents (" + keyCount + ") is > total documents " +
                    "fetched for benchmark queries (" + docCount + ").");
        }
        Set<String> keys = new HashSet<>(keyCount);

        int currPartition = 0;
        int currPos = 0;

        while(keys.size() < keyCount){
            keys.add(idsPerPartition.get(currPartition).get(currPos));  // pick a key in this partition

            // loop through the partitions
            currPartition++;
            if(currPartition >= idsPerPartition.size()){
                currPartition = 0;  // start from the beginning
                currPos++; // get the element at currPos from each partition
            }
        }

        return keys;
    }
}
