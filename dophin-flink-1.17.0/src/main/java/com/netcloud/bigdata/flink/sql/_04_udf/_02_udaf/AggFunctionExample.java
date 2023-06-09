package com.netcloud.bigdata.flink.sql._04_udf._02_udaf;

import com.netcloud.bigdata.flink.sql._04_udf._01_udf.HashCodeFunction;
import com.netcloud.bigdata.flink.sql.bean.Order;
import com.netcloud.bigdata.flink.sql.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author netcloud
 * @date 2023-06-09 14:57:56
 * @email netcloudtec@163.com
 * @description
 */
public class AggFunctionExample {
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
        tableEnv.createTemporarySystemFunction("myavg", MyAvg.class);

        //TableAPI
        table.groupBy($("id"))
                .select($("id"),call("myavg",$("vc")))
                .execute()
                .print();

        //SQL
        tableEnv.executeSql("select id, myavg(vc) from "+table +" group by id").print();
    }
}
