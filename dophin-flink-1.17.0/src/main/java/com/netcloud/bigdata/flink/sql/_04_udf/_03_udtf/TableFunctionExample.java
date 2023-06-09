package com.netcloud.bigdata.flink.sql._04_udf._03_udtf;

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
 * @date 2023-06-09 14:34:39
 * @email netcloudtec@163.com
 * @description
 * Table中执行execute().print()会报错，解决方案就是转为流然后输出查看。
 * 表函数: 1-n
 */
public class TableFunctionExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Order> tuple3DataStream = env.fromCollection(Arrays.asList(
                new Order("10000", "Andy Jerry", 1627142400000L),  // 北京时间: 2021-07-25 00:00:00
                new Order("10001","Tome Kooie", 1627153140000L),// 北京时间: 2021-07-25 02:59:00
                new Order("10002","Haddy Better", 1627167600000L),// 北京时间: 2021-07-25 07:00:00
                new Order("10003","Boby Bad", 1627171140000L)// 北京时间: 2021-07-25 07:59:00
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
        tableEnv
                .from("source_table")
                .joinLateral(call(SplitFunction.class, $("name")))
                .select($("name"), $("word"), $("length"),$("orderNo"));

        tableEnv
                .from("source_table")
                .leftOuterJoinLateral(call(SplitFunction.class, $("name")))
                .select($("name"), $("word"), $("length"),$("orderNo"));

        // 在 Table API 里重命名函数字段
        tableEnv
                .from("source_table")
                .leftOuterJoinLateral(call(SplitFunction.class, $("name")).as("newWord", "newLength"))
                .select($("name"), $("newWord"), $("newLength"),$("orderNo"));

        // 注册函数
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // 在 Table API 里调用注册好的函数
        tableEnv
                .from("source_table")
                .joinLateral(call("SplitFunction", $("name")))
                .select($("myField"), $("word"), $("length"));
        tableEnv
                .from("source_table")
                .leftOuterJoinLateral(call("SplitFunction", $("name")))
                .select($("name"), $("word"), $("length"));

        // 在 SQL 里调用注册好的函数
        tableEnv.sqlQuery(
                "SELECT name, word, length " +
                        "FROM source_table, LATERAL TABLE(SplitFunction(name))");
        tableEnv.sqlQuery(
                "SELECT name, word, length " +
                        "FROM source_table " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(name)) ON TRUE");

        // 在 SQL 里重命名函数字段
        tableEnv.sqlQuery(
                "SELECT name, newWord, newLength " +
                        "FROM source_table " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(name)) AS T(newWord, newLength) ON TRUE");
    }
}
