package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._01_with;

import com.netcloud.bigdata.flink.sql.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;


/**
 * @author netcloud
 * @date 2023-05-19 11:46:08
 * @email netcloudtec@163.com
 * @description
 * WITH 子句提供了一种用于更大查询而编写辅助语句的方法。这些编写的语句通常被称为公用表表达式，
 * 表达式可以理解为仅针对某个查询而存在的临时视图。
 */
public class WITHExample {
    public static void main(String[] args) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            DataStream<Event> tuple3DataStream = env.fromCollection(Arrays.asList(
                    new Event("2", 1L, 1627142400000L),  // 北京时间: 2021-07-25 00:00:00
                    new Event("3", 301L, 1627153140000L),// 北京时间: 2021-07-25 02:59:00
                    new Event("2", 101L, 1627167600000L),// 北京时间: 2021-07-25 07:00:00
                    new Event("2", 301L, 1627171140000L),// 北京时间: 2021-07-25 07:59:00
                    new Event("4", 201L, 1627171200000L),// 北京时间: 2021-07-25 08:00:00
                    new Event("2", 301L, 1627174740000L),// 北京时间: 2021-07-25 08:59:00
                    new Event("2", 301L, 1627228800000L),// 北京时间: 2021-07-26 00:00:00
                    new Event("2", 301L, 1627257540000L),// 北京时间: 2021-07-26 07:59:00
                    new Event("2", 1L, 1627257600000L), // 北京时间： 2021-07-26 08:00:00
                    new Event("2", 301L, 1627315140000L)));// 北京时间：2021-07-26 23:59:00
        Table table = tableEnv.fromDataStream(tuple3DataStream,
                Schema.newBuilder()
                        .column("status", "STRING")
                        .column("id", "BIGINT")
                        .column("timestamp", "BIGINT")
                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`/1000)) AS TIMESTAMP_LTZ(3))")
                        .watermark("rowtime", "rowtime - interval '5' SECOND ")
                        .build());
        tableEnv.createTemporaryView("source_table", table);

        String selectWhereSql = "WITH source_table_emp AS ( SELECT status,id FROM source_table )" +
                "SELECT status,\n"
                + "COUNT(1)\n"
                + "FROM source_table_emp group by status\n"
                ;
        tableEnv.sqlQuery(selectWhereSql).execute().print();
    }
}
