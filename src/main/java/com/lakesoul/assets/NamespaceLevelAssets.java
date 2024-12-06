package com.lakesoul.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class NamespaceLevelAssets extends KeyedProcessFunction<String, TableCountsWithTableInfo, NameSpaceCount>{

    private MapState<String,TableCounts> tableCountsMapState;
    private ValueState<NameSpaceCount> nameSpaceCountValueState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TableCounts> tableStateDescriptor =
                new MapStateDescriptor<>("tableCountsMapState",
                        String.class,
                        TableCounts.class);


        ValueStateDescriptor<NameSpaceCount> databaseStateDescriptor =
                new ValueStateDescriptor<>("nameSpaceCountValueState", NameSpaceCount.class);

        tableCountsMapState = getRuntimeContext().getMapState(tableStateDescriptor);
        nameSpaceCountValueState = getRuntimeContext().getState(databaseStateDescriptor);

    }
    @Override
    public void processElement(TableCountsWithTableInfo tableCountsWithTableInfo, KeyedProcessFunction<String, TableCountsWithTableInfo, NameSpaceCount>.Context context, Collector<NameSpaceCount> collector) throws Exception {
        String namespace = tableCountsWithTableInfo.namespace;
        int fileCount = tableCountsWithTableInfo.fileCount;
        int fileBaseCount = tableCountsWithTableInfo.fileBaseCount;
        int partitionCount = tableCountsWithTableInfo.partionCount;
        long fileBaseSize = tableCountsWithTableInfo.fileBaseSize;
        long fileSize = tableCountsWithTableInfo.fileSize;
        String tableId = tableCountsWithTableInfo.tableId;

        //查询当前状态下的统计

        NameSpaceCount nameSpaceCount = nameSpaceCountValueState.value();
        int currentPartitionCount = nameSpaceCount == null? 0 : nameSpaceCount.partitionCounts;
        int currentFileCount = nameSpaceCount == null ? 0: nameSpaceCount.fileCounts;
        int currentFileBaseCount = nameSpaceCount == null? 0 : nameSpaceCount.fileBaseCount;
        int currentTableCount = nameSpaceCount == null? 0 : nameSpaceCount.tableCounts;
        long currentFileSize = nameSpaceCount == null? 0 : nameSpaceCount.fileTotalSize;
        long currentFileBaseSize = nameSpaceCount == null? 0 : nameSpaceCount.fileBaseSize;

        if (!tableCountsMapState.contains(tableId)){
            NameSpaceCount newNameSpaceCount = new NameSpaceCount(namespace,currentTableCount + 1, currentFileCount + fileCount,
                    currentFileBaseCount + fileBaseCount,currentPartitionCount + partitionCount,currentFileSize + fileSize, currentFileBaseSize + fileBaseSize);
            nameSpaceCountValueState.update(newNameSpaceCount);
            collector.collect(newNameSpaceCount);
        } else {
            int oldPartitionCounts = tableCountsMapState.get(tableId).partitionCounts;
            long oldBaseFileSize = tableCountsMapState.get(tableId).baseFileSize;
            long oldFileSize = tableCountsMapState.get(tableId).totalFileSize;
            int oldBaseFileCounts = tableCountsMapState.get(tableId).baseFileCounts;
            int oldFileCounts = tableCountsMapState.get(tableId).totalFileCounts;
            int tableCounts = currentTableCount;
            if (partitionCount == 0){
                tableCounts = tableCounts - 1;
                tableCountsMapState.remove(tableId);
            }
            NameSpaceCount newNameSpaceCount = new NameSpaceCount(namespace,tableCounts,currentFileCount + fileCount - oldFileCounts,
                    currentFileBaseCount + fileBaseCount - oldBaseFileCounts,
                    currentPartitionCount + partitionCount - oldPartitionCounts,
                    currentFileSize + fileSize - oldFileSize, currentFileBaseSize + fileBaseSize - oldBaseFileSize);
            nameSpaceCountValueState.update(newNameSpaceCount);
            collector.collect(newNameSpaceCount);
        }
        TableCounts tableCounts = new TableCounts(tableId,partitionCount,fileBaseCount,fileCount,fileBaseSize,fileSize);
        tableCountsMapState.put(tableId,tableCounts);
    }
}
