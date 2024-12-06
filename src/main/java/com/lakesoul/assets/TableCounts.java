package com.lakesoul.assets;

public class TableCounts {
    String tableId;
    int partitionCounts;
    int baseFileCounts;
    int totalFileCounts;
    long baseFileSize;
    long totalFileSize;

    public TableCounts(String tableId, int partitionCounts, int baseFileCounts, int totalFileCounts, long baseFileSize, long totalFileSize) {
        this.tableId = tableId;
        this.partitionCounts = partitionCounts;
        this.baseFileCounts = baseFileCounts;
        this.totalFileCounts = totalFileCounts;
        this.baseFileSize = baseFileSize;
        this.totalFileSize = totalFileSize;
    }

    @Override
    public String toString() {
        return "TableCounts{" +
                "tableId='" + tableId + '\'' +
                ", partitionCounts=" + partitionCounts +
                ", baseFileCounts=" + baseFileCounts +
                ", totalFileCounts=" + totalFileCounts +
                ", baseFileSize=" + baseFileSize +
                ", totalFileSize=" + totalFileSize +
                '}';
    }
}
