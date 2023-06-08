package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._03_temporal_join;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * @author netcloud
 * @date 2023-05-29 15:55:48
 * @email netcloudtec@163.com
 * @description
 * 快照版本表的事件时间必须小于等于左表的事件时间才能关联上
 */
public class TemporalJoinEventTimeExample2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple3<Long, String, String>> orderData = Lists.newArrayList();
        orderData.add(new Tuple3<>(2L, "Euro", "2021-07-25 00:00:02"));
        orderData.add(new Tuple3<>(1L, "US Dollar", "2021-07-25 00:00:04"));
        orderData.add(new Tuple3<>(50L, "Yen", "2021-07-25 00:00:05"));
        orderData.add(new Tuple3<>(3L, "Euro", "2021-07-25 00:00:07"));
        orderData.add(new Tuple3<>(5L, "Euro", "2021-07-25 00:00:10"));

        DataStream<Tuple3<Long, String, String>> orderStream = env.fromCollection(orderData);
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Tuple3<Long, String, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(10000))
//                                .withTimestampAssigner((event, timestamp) -> event.f2.getTime())
//                );
        Table orderTable = tEnv.fromDataStream(orderStream, Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                .watermark("rowtime", "rowtime - interval '5' SECOND")
                .build());

        tEnv.createTemporaryView("Orders", orderTable);

//        orderTable.printSchema();//only for debug
        tEnv.from("Orders").execute().print(); // only for debug

        List<Tuple3<String, Long, String>> rateHistoryData = Lists.newArrayList();
        rateHistoryData.add(new Tuple3<>("Euro", 114L, "2021-07-24 00:00:02"));
        rateHistoryData.add(new Tuple3<>("US Dollar", 102L, "2021-07-25 00:00:04"));
        rateHistoryData.add(new Tuple3<>("Yen", 1L, "2021-07-25 00:00:05"));
        rateHistoryData.add(new Tuple3<>("Euro", 116L, "2021-07-25 00:00:06"));
        rateHistoryData.add(new Tuple3<>("Euro", 119L, "2021-07-25 00:00:09"));

        DataStream<Tuple3<String, Long, String>> rateStream = env.fromCollection(rateHistoryData);
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                // here  <Tuple3< String, Long, Timestamp> is need for using withTimestampAssigner
//                                .<Tuple3<String, Long, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(1000))
//                                .withTimestampAssigner((event, timestamp) -> event.f2.getTime())
//                );
        Table rateTable = tEnv.fromDataStream(
                rateStream,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                        .watermark("rowtime","rowtime - interval '5' SECOND")
                        .primaryKey("f0")
                        .build());

        tEnv.createTemporaryView("RatesHistory", rateTable);

        rateTable.printSchema();//only for debug
        tEnv.from("RatesHistory").execute().print();

        String sqlQuery2 =
                "SELECT o.f1 as currency, o.f0 as amount, o.rowtime, r.f1 as rate, " +
                        " o.f0 * r.f1 as amount_sum " +
                        "from " +
                        " Orders AS o " +
                        " LEFT JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r " +
                        "ON o.f1 = r.f0";
        tEnv.sqlQuery(sqlQuery2).execute().print();
    }
}
