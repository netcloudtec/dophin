package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._11_topn;

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
 * @date 2023-05-31 18:06:11
 * @email netcloudtec@163.com
 * @description
 * FlinkSQL TopN
 * 场景:统计关键词点击率前3名
 * 当同一个Key下第四条数据到来，如果比之前的排序数值大，将之前的小数据回撤
 * +----+--------------------------------+-------------+-------------------------+
 * | op |                            key |  search_cnt |               timestamp |
 * +----+--------------------------------+-------------+-------------------------+
 * | +I |                          Flink |        2000 | 2023-06-01 08:00:01.000 |
 * | +I |                          Flink |        3000 | 2023-06-01 08:00:02.000 |
 * | +I |                          Flink |        5000 | 2023-06-01 08:00:05.000 |
 * | -D |                          Flink |        2000 | 2023-06-01 08:00:01.000 |
 * | +I |                          Flink |        4000 | 2023-06-01 08:00:07.000 |
 * +----+--------------------------------+-------------+-------------------------+
 *
 * 如果是升序，输出结果如下（最大的数值被撤回）
 * +----+--------------------------------+-------------+-------------------------+
 * | op |                            key |  search_cnt |               timestamp |
 * +----+--------------------------------+-------------+-------------------------+
 * | +I |                          Flink |        2000 | 2023-06-01 08:00:01.000 |
 * | +I |                          Flink |        3000 | 2023-06-01 08:00:02.000 |
 * | +I |                          Flink |        5000 | 2023-06-01 08:00:05.000 |
 * | -D |                          Flink |        5000 | 2023-06-01 08:00:05.000 |
 * | +I |                          Flink |        4000 | 2023-06-01 08:00:07.000 |
 * +----+--------------------------------+-------------+-------------------------+
 */
public class TopNExample2 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple3<String, Integer, String>> orderData = Lists.newArrayList();
        orderData.add(new Tuple3<>("Flink", 2000, "2023-06-01 08:00:01"));
        orderData.add(new Tuple3<>("Flink", 3000, "2023-06-01 08:00:02"));
        orderData.add(new Tuple3<>("Flink", 5000, "2023-06-01 08:00:05"));
        orderData.add(new Tuple3<>("Flink", 4000, "2023-06-01 08:00:07"));

        DataStream<Tuple3<String, Integer, String>> orderStream = env.fromCollection(orderData);

        Table keyWordTable = tEnv.fromDataStream(orderStream, Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                .watermark("rowtime", "rowtime - interval '0' SECOND")
                .build());

        tEnv.createTemporaryView("Key_word", keyWordTable);
        keyWordTable.printSchema();//only for debug
        String sql = "SELECT key, search_cnt, rowtime as `timestamp`\n" +
                "FROM (\n" +
                "   SELECT f0 AS key, f1 AS search_cnt, rowtime, \n" +
                "     ROW_NUMBER() OVER (PARTITION BY f0\n" +
                "       ORDER BY f1 ASC) AS rownum\n" +
                "   FROM Key_word)\n" +
                "WHERE rownum <= 3;";
        tEnv.sqlQuery(sql).execute().print();

    }
}
