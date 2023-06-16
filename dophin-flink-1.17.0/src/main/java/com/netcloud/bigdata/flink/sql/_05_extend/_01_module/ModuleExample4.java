package com.netcloud.bigdata.flink.sql._05_extend._01_module;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * @author netcloud
 * @date 2023-06-09 17:25:24
 * @email netcloudtec@163.com
 * @description
 * 加载Kafka中的json字符串并加载hive module使用其get_json_object
 */
public class ModuleExample4 {
    public static void main(String[] args)  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 1.source-kafka-test主题
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("xxx:9092")
                .setTopics("autocar_Madvisor")
                .setGroupId("autocar_Madvisor_online03")
                //.setStartingOffsets(OffsetsInitializer.earliest())//消费起始位移直接选择为"最早"
                //.setStartingOffsets(OffsetsInitializer.latest())//消费起始位移直接选择为"最新 "
                //.setStartingOffsets(OffsetsInitializer.offsets(Map< TopicPartition,Long>))//消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))//消费起始位移选择之前提交的偏移量（如果没有则重置为LATEST）
                .setValueOnlyDeserializer(new SimpleStringSchema())//读取文件的反序列化Schema
                // 开启了kafka底层消费者的自动位移提交机制，它会把最新的消费位移提交到kafka的consumer_offet中
                //在新api中及时开启了自动位移提交机制，kafka依然不依赖自动位移提交机制（宕机重启时，优先从flink自己的状态中去获取偏移量（更可靠））
                //.setProperty("enable.auto.commit", "true")
                .build();
        //在这里要注意： 如果kafka只接收数据，从来没来消费过，程序一开始不要用latest，不然以前的数据就接收不到了。应当先earliest，然后二都都可以 。
        //1)代码端先earliest，最早提交的数据是可以获取到的，再生产数据也是可以获取到的。
        //2)将auto.offset.reset设置成latest，再生产数据也是可以获取到的。
        //3)虽然auto.offset.reset默认是latest，但是建议使用earliest。
        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        Table sourceTable = tableEnv.fromDataStream(kafka_source);
        //注册表
        tableEnv.createTemporaryView("source_table", sourceTable);
        //加载并启用hive module
        tableEnv.loadModule("hive",new HiveModule("2.3.3"));
        tableEnv.sqlQuery("SELECT get_json_object(f0,'$.ip')  FROM source_table").execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
