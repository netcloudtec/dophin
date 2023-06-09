package com.netcloud.bigdata.flink.sql._04_udf._04_udtaf;

import com.netcloud.bigdata.flink.sql.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * @author netcloud
 * @date 2023-06-09 15:55:43
 * @email netcloudtec@163.com
 * @description 提取每个WaterSensor最高的两个水位值。
 * 用户定义的表聚合函数（User-Defined Table Aggregate Functions，UDTAGGs），可以把一个表中数据，
 * 聚合为具有多行和多列的结果表。这跟AggregateFunction非常类似，只是之前聚合结果是一个标量值，现在变成了一张表。
 */
public class UDTAFExample {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);
        //4先注册再使用
        tableEnv.createTemporarySystemFunction("Top2", new Top2());
        //TableAPI
        table.groupBy($("id")).flatAggregate(call("Top2", $("vc")).as("top", "rank"))
                .select($("id"), $("top"), $("rank"))
                .execute()
                .print();

    }
}
