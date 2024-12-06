package com.lakesoul.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DomainLevelAssets extends KeyedProcessFunction<String, TableCountsWithTableInfo, DomainCount> {
    private MapState<String,TableCounts> tableCountsMapState;
    private ValueState<DomainCount> domainCountValueState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TableCounts> tableStateDescriptor =
                new MapStateDescriptor<>("tableCountsMapState",
                        String.class,
                        TableCounts.class);


        ValueStateDescriptor<DomainCount> databaseStateDescriptor =
                new ValueStateDescriptor<>("nameSpaceCountValueState", DomainCount.class);

        tableCountsMapState = getRuntimeContext().getMapState(tableStateDescriptor);
        domainCountValueState = getRuntimeContext().getState(databaseStateDescriptor);

    }
    @Override
    public void processElement(TableCountsWithTableInfo tableCountsWithTableInfo, KeyedProcessFunction<String, TableCountsWithTableInfo, DomainCount>.Context context, Collector<DomainCount> collector) throws Exception {

        String domain = tableCountsWithTableInfo.domain;
        int fileCount = tableCountsWithTableInfo.fileCount;
        int fileBaseCount = tableCountsWithTableInfo.fileBaseCount;
        int partitionCount = tableCountsWithTableInfo.partionCount;
        long fileBaseSize = tableCountsWithTableInfo.fileBaseSize;
        long fileSize = tableCountsWithTableInfo.fileSize;
        String tableId = tableCountsWithTableInfo.tableId;

        DomainCount domainCount = domainCountValueState.value();
        int currentPartitionCount = domainCount == null? 0 : domainCount.partitionCounts;
        int currentFileCount = domainCount == null ? 0: domainCount.fileCounts;
        int currentFileBaseCount = domainCount == null? 0 : domainCount.fileBaseCount;
        int currentTableCount = domainCount == null? 0 : domainCount.tableCounts;
        long currentFileSize = domainCount == null? 0 : domainCount.fileTotalSize;
        long currentFileBaseSize = domainCount == null? 0 : domainCount.fileBaseSize;

        if (!tableCountsMapState.contains(tableId)){
            DomainCount newDomainCount = new DomainCount(domain,currentTableCount + 1, currentFileCount + fileCount,
                    currentFileBaseCount + fileBaseCount,currentPartitionCount + partitionCount,currentFileSize + fileSize, currentFileBaseSize + fileBaseSize);
            domainCountValueState.update(newDomainCount);
            collector.collect(newDomainCount);
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
            DomainCount newDomainCount = new DomainCount(domain,tableCounts,currentFileCount + fileCount - oldFileCounts,
                    currentFileBaseCount + fileBaseCount - oldBaseFileCounts,
                    currentPartitionCount + partitionCount - oldPartitionCounts,
                    currentFileSize + fileSize - oldFileSize, currentFileBaseSize + fileBaseSize - oldBaseFileSize);
            domainCountValueState.update(newDomainCount);
            collector.collect(newDomainCount);
        }
        TableCounts tableCounts = new TableCounts(tableId,partitionCount,fileBaseCount,fileCount,fileBaseSize,fileSize);
        tableCountsMapState.put(tableId,tableCounts);
    }
}
