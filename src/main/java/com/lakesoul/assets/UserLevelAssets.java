package com.lakesoul.assets;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserLevelAssets extends KeyedProcessFunction<String, TableCountsWithTableInfo, UserCounts> {
    private MapState<String,TableCounts> tableCountsMapState;
    private ValueState<UserCounts> userCountValueState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TableCounts> tableStateDescriptor =
                new MapStateDescriptor<>("tableCountsMapState",
                        String.class,
                        TableCounts.class);


        ValueStateDescriptor<UserCounts> databaseStateDescriptor =
                new ValueStateDescriptor<>("userCountValueState", UserCounts.class);

        tableCountsMapState = getRuntimeContext().getMapState(tableStateDescriptor);
        userCountValueState = getRuntimeContext().getState(databaseStateDescriptor);

    }
    @Override
    public void processElement(TableCountsWithTableInfo tableCountsWithTableInfo, KeyedProcessFunction<String, TableCountsWithTableInfo, UserCounts>.Context context, Collector<UserCounts> collector) throws Exception {

        System.out.println(tableCountsWithTableInfo);
        String user = tableCountsWithTableInfo.creator;
        int fileCount = tableCountsWithTableInfo.fileCount;
        int fileBaseCount = tableCountsWithTableInfo.fileBaseCount;
        int partitionCount = tableCountsWithTableInfo.partionCount;
        long fileBaseSize = tableCountsWithTableInfo.fileBaseSize;
        long fileSize = tableCountsWithTableInfo.fileSize;
        String tableId = tableCountsWithTableInfo.tableId;

        UserCounts userCount = userCountValueState.value();
        int currentPartitionCount = userCount == null? 0 : userCount.partitionCounts;
        int currentFileCount = userCount == null ? 0: userCount.fileCounts;
        int currentFileBaseCount = userCount == null? 0 : userCount.fileBaseCount;
        int currentTableCount = userCount == null? 0 : userCount.tableCounts;
        long currentFileSize = userCount == null? 0 : userCount.fileTotalSize;
        long currentFileBaseSize = userCount == null? 0 : userCount.fileBaseSize;

        if (!tableCountsMapState.contains(tableId) && partitionCount > 0){
            UserCounts newUserCounts = new UserCounts(user,currentTableCount + 1, currentFileCount + fileCount,
                    currentFileBaseCount + fileBaseCount,currentPartitionCount + partitionCount,currentFileSize + fileSize, currentFileBaseSize + fileBaseSize);
            userCountValueState.update(newUserCounts);
            collector.collect(newUserCounts);
        } else {
            int oldPartitionCounts = tableCountsMapState.get(tableId).partitionCounts;
            long oldBaseFileSize = tableCountsMapState.get(tableId).baseFileSize;
            long oldFileSize = tableCountsMapState.get(tableId).totalFileSize;
            int oldBaseFileCounts = tableCountsMapState.get(tableId).baseFileCounts;
            int oldFileCounts = tableCountsMapState.get(tableId).totalFileCounts;
            int tableCounts = currentTableCount;
            if (partitionCount == 0 && oldPartitionCounts > 0 && fileCount ==0 && fileSize == 0){
                tableCounts = tableCounts - 1;
                tableCountsMapState.remove(tableId);
            }
            UserCounts newUserCount = new UserCounts(user,tableCounts,currentFileCount + fileCount - oldFileCounts,
                    currentFileBaseCount + fileBaseCount - oldBaseFileCounts,
                    currentPartitionCount + partitionCount - oldPartitionCounts,
                    currentFileSize + fileSize - oldFileSize, currentFileBaseSize + fileBaseSize - oldBaseFileSize);
            userCountValueState.update(newUserCount);
            collector.collect(newUserCount);
        }
        TableCounts tableCounts = new TableCounts(tableId,partitionCount,fileBaseCount,fileCount,fileBaseSize,fileSize);
        if (partitionCount > 0){
            tableCountsMapState.put(tableId,tableCounts);
        }
    }
}
