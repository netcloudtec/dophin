package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._12_window_topn;

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
 * @date 2023-06-02 11:14:49
 * @email netcloudtec@163.com
 * @description
 * WindowTopN: 每个滚动窗口组取TopN
 * 如果取每个组中的TopN，应该去掉Max，使用原子字段，否则每个组只取了最大的那条数据，是个Bug。
 */
public class WindowTopNExample2 {
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
        orderData.add(new Tuple3<>("Flink", 6000, "2023-06-01 08:01:20"));
        orderData.add(new Tuple3<>("Flink", 7000, "2023-06-01 08:02:20"));

        DataStream<Tuple3<String, Integer, String>> orderStream = env.fromCollection(orderData);

        Table keyWordTable = tEnv.fromDataStream(orderStream, Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                .watermark("rowtime", "rowtime - interval '0' SECOND")
                .build());

        tEnv.createTemporaryView("Key_word", keyWordTable);
        keyWordTable.printSchema();//only for debug

        String bakSQL ="SELECT key, search_cnt, window_start, window_end\n" +
                "FROM\n" +
                "    (\n" +
                "   SELECT key\n" +
                "        ,search_cnt\n" +
                "        ,window_start\n" +
                "        ,window_end, \n" +
                "        ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key ORDER BY search_cnt desc) AS rownum\n" +
                "   FROM\n" +
                "      (\n" +
                "      SELECTwindow_start\n" +
                "      ,window_end\n" +
                "      ,f0 AS key\n" +
                "      ,max(f1) as search_cnt\n" +
                "      FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(rowtime), INTERVAL '1' MINUTES))\n" +
                "      GROUP BY window_start\n" +
                "              ,window_end\n" +
                "              ,f0\n" +
                "      )\n" +
                ")\n" +
                "WHERE rownum <= 3;";

        String sql = "SELECT  key\n" +
                "       ,search_cnt\n" +
                "       ,window_start\n" +
                "       ,window_end\n" +
                "FROM\n" +
                "    (\n" +
                "    SELECT  key\n" +
                "            ,search_cnt\n" +
                "            ,window_start\n" +
                "            ,window_end\n" +
                "            ,ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key ORDER BY search_cnt desc) AS rownum\n" +
                "    FROM\n" +
                "        (\n" +
                "        SELECT  window_start\n" +
                "                ,window_end\n" +
                "                ,f0 AS key\n" +
                "                ,f1 AS search_cnt\n" +
                "        FROM TABLE(TUMBLE(TABLE Key_word, DESCRIPTOR(rowtime), INTERVAL '1' MINUTES))\n" +
                "        GROUP BY window_start\n" +
                "                ,window_end\n" +
                "                ,f0\n" +
                "                ,f1\n" +

                "        )\n" +
                "    )\n" +
                "WHERE rownum <= 3;";
        tEnv.sqlQuery(sql).execute().print();
    }
}
