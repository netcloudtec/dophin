package com.netcloud.bigdata.flink.sql._06_connector;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author netcloud
 * @date 2023-06-13 14:45:13
 * @email netcloudtec@163.com
 * @description
 */
public class KafkaToClickhouseConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToClickhouseConnector.class);

    private static String bootstrapServers;

    private static String groupId;

    private static String autoOffsetReset;

    private static String networkMonitorTopic;

    private static boolean flinkCheckpoint;

    private static String flinkCheckpointDir;

    private static String partitionDiscoveryInterval;

    private static long checkPointTimeout;

    private static long restartBetweenTime;

    private static int restartNum;

    private static long checkpointsBetweenTime;

    private static int maxConcurrentCheckpoints;

    /**
     * batchIntervalMs:写入clickhouse中的每一个批次的时间间隔
     * batchSize:写入clickhosue的每个批次的大小
     * maxRetries:写入clickhouse的重试次数
     */
    private static int batchIntervalMs;
    private static int batchSize;
    private static int maxRetries;

    /**
     * clickhouse的JDBC连接配置
     */
    private static String clickhouseJdbcUrl;
    private static String clickhouseJdbcDriver;

    static {
        Configurations configs = new Configurations();
        try {
            PropertiesConfiguration config = configs.properties(new File("/Users/yangshaojun/ZDauto/dophin/dophin-autocar-performance/src/main/resources/config/dev/application.properties"));
            bootstrapServers = config.getString("network.bootstrap.servers");
            groupId = config.getString("network.group.id");
            autoOffsetReset = config.getString("network.auto.offset.reset");

            networkMonitorTopic = config.getString("network.topic");

            flinkCheckpoint = config.getBoolean("flink.checkpoint");
            flinkCheckpointDir = config.getString("flink.checkpoint.dir");
            checkPointTimeout = config.getLong("flink.checkpoint.timeout");
            partitionDiscoveryInterval = config.getString("flink.partition.discovery.interval");
            restartBetweenTime = config.getLong("flink.restart.between.time");
            restartNum = config.getInt("flink.restart.num");
            checkpointsBetweenTime = config.getLong("flink.checkpoints.between.time");
            maxConcurrentCheckpoints = config.getInt("flink.max.concurrent.checkpoints");
            batchIntervalMs = config.getInt("clickhouse.jdbc.batchIntervalMs");
            batchSize = config.getInt("clickhouse.jdbc.batchSize");
            maxRetries = config.getInt("clickhouse.jdbc.maxRetries");
            clickhouseJdbcDriver = config.getString("clickhouse.jdbc.driver");
            clickhouseJdbcUrl = config.getString("clickhouse.jdbc.url");

        } catch (ConfigurationException e) {
            LOGGER.error("read application.properties error..", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        //kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", autoOffsetReset);
        // partition discovery
        properties.setProperty("flink.partition-discovery.interval-millis", partitionDiscoveryInterval);

        //运行环境并设置checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        long checkpointIntervalMS = 300000;
        env.enableCheckpointing(checkpointIntervalMS);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
        //设置Flink的重启策略,重试4次,每次间隔1秒
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(restartNum, restartBetweenTime));
        //确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointsBetweenTime);
        //同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);

//        try {
//            env.getConfig().addDefaultKryoSerializer(Class.forName("java.util.Collections$UnmodifiableCollection"), UnmodifiableCollectionsSerializer.class);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }

//        if (flinkCheckpoint) {
//            env.setStateBackend(new EmbeddedRocksDBStateBackend());
//            env.getCheckpointConfig().setCheckpointStorage(flinkCheckpointDir);
//            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        }

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(networkMonitorTopic, new SimpleStringSchema(), properties);
        DataStream<String> dataStream = env.addSource(consumer).name("SOURCE_NETWORK_STREAMING");
        Table sourceTable = tableEnv.fromDataStream(dataStream).renameColumns($("f0").as("content"));;
        tableEnv.createTemporaryView("network_source_table", sourceTable);
        tableEnv.loadModule("hive",new HiveModule("2.3.3"));

        String sinkTableSQL = "CREATE TABLE sink_table (\n" +
                " car_plate String,\n" +
                " ip_node String,\n" +
                " event_time String,\n" +
                " eth0_RxBytes String,\n" +
                " eth0_RxRxErrors String,\n" +
                " eth0_RxDropped String,\n" +
                " eth0_RxSpeed String,\n" +
                " eth0_TxBytes String,\n" +
                " eth0_TxErrors String,\n" +
                " eth0_TxDropped String,\n" +
                " eth0_TxSpeed String,\n" +
                " docker_ps_bash_pid String,\n" +
                " controller_pid String,\n" +
                " dongFeng_E70_can_adapter_pid String,\n" +
                " drivers_camera_sensing_node_pid String,\n" +
                " drivers_gnss_node_pid String,\n" +
                " drivers_hesai128_node_pid String,\n" +
                " drivers_robosense_node_pid String,\n" +
                " hadmap_engine_node_pid String,\n" +
                " hadmap_server_pid String,\n" +
                " jinlv_can_adapter_pid String,\n" +
                " local_planning_pid String,\n" +
                " localization_node_pid String,\n" +
                " lslidar_c32_decoder_node_pid String,\n" +
                " lslidar_c32_driver_node_pid String,\n" +
                " map_matching_node_pid String,\n" +
                " perception_camera_2D_front_node_pid String,\n" +
                " perception_camera_2D_side_node_pid String,\n" +
                " perception_camera_3D_node_pid String,\n" +
                " perception_camera_SEG_node_pid String,\n" +
                " perception_fusion2_node_pid String,\n" +
                " prediction_node_pid String,\n" +
                " pretreatment_node_pid String,\n" +
                " record_cache_node_pid String,\n" +
                " rosout_pid String,\n" +
                " rs_perception_node_pid String,\n" +
                " telematics_node_pid String,\n" +
                " timesync_node_pid String,\n" +
                " xiaoba_fusion_node_pid String,\n" +
                " agent_name String,\n" +
                " input_type String,\n" +
                " sn String,\n" +
                " event_timestamp String,\n" +
                " fields_type String,\n" +
                " host_name String,\n" +
                " car_type String,\n" +
                " monitor_type String\n" +
                ") WITH (\n" +
                "  'connector' = 'clickhouse',\n" +
                "  'url' = 'clickhouse://10.2.9.124:8123',\n" +
                "  'database-name' = 'default',\n" +
                "  'table-name' = 'auto_car_network_monitor',\n" +
                "  'sink.batch-size' = '500', \n" +
                "  'sink.flush-interval' = '10000',\n" +
                "  'sink.max-retries' = '3'\n" +
                ")\n";

        String executeSql ="INSERT INTO sink_table\n"+
                "SELECT  get_json_object(content,'$.carNum')                                                                                                AS   car_plate\n" +
                "       ,get_json_object(content,'$.ip')                                                                                                    AS   ip_node\n" +
                "       ,FROM_UNIXTIME(CAST(get_json_object(content,'$.time') AS BIGINT)/1000,'yyyy-MM-dd HH:mm:ss')                                        AS   event_time\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.RxBytes')   ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.RxBytes')    ,'\\\"')[2])     AS   eth0_RxBytes\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.RxRxErrors'),'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.RxRxErrors') ,'\\\"')[2])     AS   eth0_RxRxErrors\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.RxDropped') ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.RxDropped')  ,'\\\"')[2])     AS   eth0_RxDropped\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.RxSpeed')   ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.RxSpeed')    ,'\\\"')[2])     AS   eth0_RxSpeed\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.TxBytes')   ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.TxBytes')    ,'\\\"')[2])     AS   eth0_TxBytes\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.TxErrors')  ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.TxErrors')   ,'\\\"')[2])     AS   eth0_TxErrors\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.TxDropped') ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.TxDropped')  ,'\\\"')[2])     AS   eth0_TxDropped\n" +
                "       ,COALESCE(split(get_json_object(content,'$.modules.eyeinthesky-default-1.docker_network.eth0.TxSpeed')   ,'\\\"')[2],split(get_json_object(content,'$.modules.eye_in_the_sky.docker_network.eth0.TxSpeed')    ,'\\\"')[2])     AS   eth0_TxSpeed\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.bash.pid') ,'\\[|\\]|\\\"','')                        AS   docker_ps_bash_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.controller.pid')     ,'\\[|\\]|\\\"','')              AS   controller_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.DongFeng_E70_ca.pid'),'\\[|\\]|\\\"','')              AS   DongFeng_E70_can_adapter_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.drivers_camera_.pid'),'\\[|\\]|\\\"','')              AS   drivers_camera_sensing_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.drivers_gnss_no.pid'),'\\[|\\]|\\\"','')              AS   drivers_gnss_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.drivers_hesai12.pid'),'\\[|\\]|\\\"','')              AS   drivers_hesai128_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.drivers_robosen.pid'),'\\[|\\]|\\\"','')              AS   drivers_robosense_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.hadmap_engine_n.pid'),'\\[|\\]|\\\"','')              AS   hadmap_engine_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.hadmap_server.pid')  ,'\\[|\\]|\\\"','')              AS   hadmap_server_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.jinlv_can_adapt.pid'),'\\[|\\]|\\\"','')              AS   jinlv_can_adapter_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.local_planning.pid') ,'\\[|\\]|\\\"','')              AS   local_planning_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.localization_no.pid'),'\\[|\\]|\\\"','')              AS   localization_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.lslidar_c32_dec.pid'),'\\[|\\]|\\\"','')              AS   lslidar_c32_decoder_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.lslidar_c32_dri.pid'),'\\[|\\]|\\\"','')              AS   lslidar_c32_driver_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.map_matching_no.pid'),'\\[|\\]|\\\"','')              AS   map_matching_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.perception_came.pid'),'\\[|\\]|\\\"','')              AS   perception_camera_2D_front_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.perception_came.pid'),'\\[|\\]|\\\"','')              AS   perception_camera_2D_side_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.perception_came.pid'),'\\[|\\]|\\\"','')              AS   perception_camera_3D_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.perception_came.pid'),'\\[|\\]|\\\"','')              AS   perception_camera_SEG_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.perception_fusi.pid'),'\\[|\\]|\\\"','')              AS   perception_fusion2_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.prediction_node.pid'),'\\[|\\]|\\\"','')              AS   prediction_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.pretreatment_no.pid'),'\\[|\\]|\\\"','')              AS   pretreatment_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.record_cache_no.pid'),'\\[|\\]|\\\"','')              AS   record_cache_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.rosout.pid')         ,'\\[|\\]|\\\"','')              AS   rosout_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.rs_perception_n.pid'),'\\[|\\]|\\\"','')              AS   rs_perception_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.telematics_node.pid'),'\\[|\\]|\\\"','')              AS   telematics_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.timesync_node.pid')  ,'\\[|\\]|\\\"','')              AS   timesync_node_pid\n" +
                "       ,regexp_replace(get_json_object(content,'$.modules.autocar_default_1.docker_ps.xiaoba_fusion_n.pid'),'\\[|\\]|\\\"','')              AS   xiaoba_fusion_node_pid\n" +
                "       ,get_json_object(content,'$.agentName')                                                                                   AS   agent_name\n" +
                "       ,get_json_object(content,'$.input.type')                                                                                  AS   input_type\n" +
                "       ,get_json_object(content,'$.sn')                                                                                          AS   sn\n" +
                "       ,get_json_object(content,'$.time')                                                                                        AS   event_timestamp\n" +
                "       ,get_json_object(content,'$.fields.type')                                                                                 AS   fields_type\n" +
                "       ,get_json_object(content,'$.host.name')                                                                                   AS   host_name\n" +
                "       ,get_json_object(content,'$.carType')                                                                                     AS   car_type\n" +
                "       ,get_json_object(content,'$.moniterType')                                                                                 AS   monitor_type\n" +
                "FROM   network_source_table";

        tableEnv.executeSql(sinkTableSQL);
        tableEnv.executeSql(executeSql).print();

//        Table table = tableEnv.sqlQuery(sql);
//        tableEnv.toChangelogStream(table).print();

        dataStream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
