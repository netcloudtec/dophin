//package com.netcloud.bigdata.flink.sql._01_basics;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @author netcloud
// * @date 2023-06-19 20:10:59
// * @email netcloudtec@163.com
// * @description
// */
//public class Test {
//    public static void main(String[] args) {
//        //TODO 创建表环境
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
//
//        //TODO kafka 数据源
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers(bootstrapServers)
//                .setTopics(kafkaTopic)
//                .setGroupId(kafkaGroupId)
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");
//
//        //TODO 将 DataStream 转为一个 Table API 中的 Table 对象进行使用
//        Table sourceTable = tEnv.fromDataStream(kafka_source).renameColumns($("f0").as("content"));
//
//        tEnv.createTemporaryView("source_table", sourceTable);
//
//        String selectWhereSql = "select content from source_table";
//        Table resultTable = tEnv.sqlQuery(selectWhereSql);
//
//        tEnv.toChangelogStream(resultTable).print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
