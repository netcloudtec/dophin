package com.netcloud.bigdata.flink.sql._04_udf._01_udf;

import com.netcloud.bigdata.flink.sql.bean.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-06-09 11:17:22
 * @email netcloudtec@163.com
 * @description
 * 如何创建一个基本的标量函数，以及如何在 Table API 和 SQL 里调用这个函数
 * 函数用于 SQL 查询前要先经过注册；而在用于 Table API 时，函数可以先注册后调用，也可以 内联 后直接使用。
 *
 */
public class UDFExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Order> tuple3DataStream = env.fromCollection(Arrays.asList(
                new Order("10000", "Andy", 1627142400000L),  // 北京时间: 2021-07-25 00:00:00
                new Order("10001","Tome", 1627153140000L),// 北京时间: 2021-07-25 02:59:00
                new Order("10002","Haddy", 1627167600000L),// 北京时间: 2021-07-25 07:00:00
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

        // 在 Table API 里不经注册直接“内联”调用函数
        Table select = tableEnv.from("source_table").select(call(HashCodeFunction.class, $("name")));

        // 注册函数
        tableEnv.createTemporarySystemFunction("HashCodeFunction", HashCodeFunction.class);

        // 在 Table API 里调用注册好的函数
        tableEnv.from("source_table").select(call("HashCodeFunction", $("name")));

        // 在 SQL 里调用注册好的函数
        tableEnv.sqlQuery("SELECT HashCodeFunction(name) FROM source_table");


        tableEnv.toChangelogStream(select).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
