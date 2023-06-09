package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._03_select_distinct;

import com.netcloud.bigdata.flink.sql.bean.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-19 14:51:49
 * @email netcloudtec@163.com
 * @description
 * 如果使用”SELECT DISTINCT“查询,所有的复制行都会从结果集(每个分组只会保留一行)中被删除.
 */
public class SelectDistinctExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Order> tuple3DataStream = env.fromCollection(Arrays.asList(
                new Order("10000", "Andy", 1627142400000L),  // 北京时间: 2021-07-25 00:00:00
                new Order("10001","Tome", 1627153140000L),// 北京时间: 2021-07-25 02:59:00
                new Order("10001","Haddy", 1627167600000L),// 北京时间: 2021-07-25 07:00:00
                new Order("10003","Boby", 1627171140000L)// 北京时间: 2021-07-25 07:59:00
        ));
        Table table = tableEnv.fromDataStream(tuple3DataStream,
                Schema.newBuilder()
                        .column("orderNo", "STRING")
                        .column("name", "STRING")
                        .column("timestamp", "BIGINT")
                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`/1000)) AS TIMESTAMP_LTZ(3))")
                        .watermark("rowtime", "rowtime - interval '5' SECOND ")
                        .build());
        // 注册临时视图
        tableEnv.createTemporaryView("source_table", table);
        // 在SQL里使用SELECT DISTINCT
        String selectSQL="SELECT DISTINCT orderNo FROM source_table";
        tableEnv.sqlQuery(selectSQL).execute().print();
    }
}
