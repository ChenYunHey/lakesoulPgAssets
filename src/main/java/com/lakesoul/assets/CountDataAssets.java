package com.lakesoul.assets;


import com.lakesoul.assets.util.SourceOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.Properties;
public class CountDataAssets {
    private static String host;
    private static String dbName;
    private static String userName;
    private static String passWord;
    private static int port;
    private static int splitSize;
    private static String slotName;
    private static String pluginName;
    private static String schemaList;
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(),SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(),SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.table_info","public.data_commit_info"};
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(host)
                        .port(port)
                        .database(dbName)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .username(userName)
                        .password(passWord)
                        .slotName(slotName)
                        .decodingPluginName(pluginName) // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(splitSize) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        // Define the source data stream
        DataStreamSource<String> postgresParallelSource = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2);


        OutputTag<Tuple3<String, String, String[]>> tableInfoTag = new OutputTag<Tuple3<String, String, String[]>>("table_info") {};
        OutputTag<Tuple3<String, String, String[]>> dataCommitInfoTag = new OutputTag<Tuple3<String, String, String[]>>("data_commit_info") {};

       SingleOutputStreamOperator<Tuple3<String, String, String[]>> mainProcess = postgresParallelSource.map(new PartitionLevelAssets.metaMapper()).process(new PartitionLevelAssets.sideProcessing());

        SingleOutputStreamOperator<TableCounts> datacommitInfoStream = mainProcess.getSideOutput(dataCommitInfoTag)
                .keyBy(value -> value.f1).process(new PartitionLevelAssets.PartitionLevelProcessFunction()).keyBy(value -> value.tableId)
                .process(new TableLevelAsstes());

        SingleOutputStreamOperator<TableCountsWithTableInfo> tableCountsStreaming = mainProcess.getSideOutput(tableInfoTag)
                .keyBy(value -> value.f1)
                .connect(datacommitInfoStream.keyBy(value -> value.tableId))
                .process(new TableLevelAsstes.MergeFunction());

        JdbcConnectionOptions build = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://localhost:5432/lakesoul_test")
                .withDriverName("org.postgresql.Driver")
                .withUsername(userName)
                .withPassword(passWord)
                .build();

        String tableLevelAssetsSql = "INSERT INTO table_level_assets (table_id, table_name, domain, creator, namespace, partition_counts, file_counts, file_total_size, file_base_count, file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (table_id) DO UPDATE SET partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size, file_base_count = EXCLUDED.file_base_count, file_base_size=EXCLUDED.file_base_size";

        SinkFunction<TableCountsWithTableInfo> sink = JdbcSink.sink(
                tableLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.tableId);
                    ps.setString(2, t.tableName);
                    ps.setString(3, t.domain);
                    ps.setString(4, t.creator);
                    ps.setString(5, t.namespace);
                    ps.setInt(6, t.partionCount);
                    ps.setInt(7, t.fileCount);
                    ps.setLong(8, t.fileSize);
                    ps.setInt(9, t.fileBaseCount);
                    ps.setLong(10,t.fileBaseSize);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build
        );
        tableCountsStreaming.addSink(sink);

        //统计namespace级别的资产
        String dataBaseLevelAssetsSql = "INSERT INTO namespace_level_assets (namespace, table_counts, partition_counts, file_counts, file_total_size, file_base_counts, file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (namespace) DO UPDATE SET table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size, file_base_counts = EXCLUDED.file_base_counts, file_base_size = EXCLUDED.file_base_size";

        SinkFunction<NameSpaceCount> namespaceAssetsSink = JdbcSink.sink(
                dataBaseLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.nameSpace);
                    ps.setInt(2, t.tableCounts);
                    ps.setInt(3, t.partitionCounts);
                    ps.setInt(4, t.fileCounts);
                    ps.setLong(5, t.fileTotalSize);
                    ps.setInt(6, t.fileBaseCount);
                    ps.setLong(7, t.fileBaseSize);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build
        );
        tableCountsStreaming.keyBy(value -> value.namespace)
                .process(new NamespaceLevelAssets())
                .addSink(namespaceAssetsSink);

        //统计domain级别的资产

        String domainLevelAssetsSql = "INSERT INTO domain_level_assets (domain, table_counts, partition_counts, file_counts, file_total_size, file_base_counts, file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (domain) DO UPDATE SET table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size, file_base_counts = EXCLUDED.file_base_counts , file_base_size = EXCLUDED.file_base_size";

        SinkFunction<DomainCount> domainAssetsSink = JdbcSink.sink(
                domainLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.domain);
                    ps.setInt(2, t.tableCounts);
                    ps.setInt(3, t.partitionCounts);
                    ps.setInt(4, t.fileCounts);
                    ps.setLong(5, t.fileTotalSize);
                    ps.setInt(6, t.fileBaseCount);
                    ps.setLong(7, t.fileBaseSize);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),
                build
        );
        tableCountsStreaming.keyBy(value -> value.domain)
                .process(new DomainLevelAssets())
                .addSink(domainAssetsSink);
        //用户级别资产统计
        String userLevelAssetsSql = "INSERT INTO user_level_assets (creator, table_counts, partition_counts, file_counts, file_total_size, file_base_counts, file_base_size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (creator) DO UPDATE SET table_counts = EXCLUDED.table_counts,partition_counts = EXCLUDED.partition_counts, file_counts = EXCLUDED.file_counts, file_total_size = EXCLUDED.file_total_size, file_base_counts = EXCLUDED.file_base_counts , file_base_size = EXCLUDED.file_base_size";

        SinkFunction<UserCounts> userLevelAssetsSink = JdbcSink.sink(
                userLevelAssetsSql,
                (ps, t) -> {
                    ps.setString(1, t.creator);
                    ps.setInt(2, t.tableCounts);
                    ps.setInt(3, t.partitionCounts);
                    ps.setInt(4, t.fileCounts);
                    ps.setLong(5, t.fileTotalSize);
                    ps.setInt(6, t.fileBaseCount);
                    ps.setLong(7, t.fileBaseSize);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(1000)
                        .withBatchSize(50)
                        .withMaxRetries(0)
                        .build(),

                build
        );
                tableCountsStreaming.keyBy(value -> value.creator)
                .process(new UserLevelAssets()).addSink(userLevelAssetsSink);
        env.execute();
    }
}
