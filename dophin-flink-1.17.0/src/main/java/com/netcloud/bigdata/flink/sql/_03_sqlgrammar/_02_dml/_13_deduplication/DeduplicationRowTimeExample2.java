package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._13_deduplication;

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
 * @date 2023-06-02 14:28:45
 * @email netcloudtec@163.com
 * @description
 * 根据事件时间降序只保留一条，回撤流
 */
public class DeduplicationRowTimeExample2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple3<String, Integer, String>> orderData = Lists.newArrayList();
        orderData.add(new Tuple3<>("user1", 1, "2023-06-01 08:00:03"));
        orderData.add(new Tuple3<>("user1", 2, "2023-06-01 08:00:02"));
        orderData.add(new Tuple3<>("user1", 6, "2023-06-01 08:00:05"));
        orderData.add(new Tuple3<>("user1", 4, "2023-06-01 08:00:07"));
        orderData.add(new Tuple3<>("user1", 5, "2023-06-01 08:01:20"));
        orderData.add(new Tuple3<>("user1", 3, "2023-06-01 08:02:20"));

        DataStream<Tuple3<String, Integer, String>> orderStream = env.fromCollection(orderData);

        Table keyWordTable = tEnv.fromDataStream(orderStream, Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                .watermark("rowtime", "rowtime - interval '0' SECOND")
                .build());

        tEnv.createTemporaryView("user_level", keyWordTable);
        keyWordTable.printSchema();//only for debug

        String sql = "SELECT  user_id\n" +
                "        ,level\n" +
                "        ,rowtime\n" +
                "FROM\n" +
                "    (\n" +
                "    SELECT  f0       AS user_id\n" +
                "            ,f1      AS level\n" +
                "            ,rowtime AS rowtime\n" +
                "           ,ROW_NUMBER() OVER(PARTITION BY f0 ORDER BY rowtime DESC) AS rn\n" +
                "    FROM user_level\n" +
                "     )\n" +
                "WHERE rn = 1;";
        tEnv.sqlQuery(sql).execute().print();

    }
}
