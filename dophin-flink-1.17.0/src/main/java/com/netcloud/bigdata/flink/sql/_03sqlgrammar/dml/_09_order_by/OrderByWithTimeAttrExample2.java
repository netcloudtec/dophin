package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._09_order_by;

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
 * @date 2023-05-31 17:11:41
 * @email netcloudtec@163.com
 * @description
 * 对延迟的数据重新升序排序，如果没有设置watermark或者设置rowtime - interval '0' SECOND
 * 延迟的数据会丢失，不再参与排序。
 */
public class OrderByWithTimeAttrExample2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple3<Long, String, String>> orderData = Lists.newArrayList();
        orderData.add(new Tuple3<>(1L, "US Dollar", "2021-07-25 00:00:04"));
        orderData.add(new Tuple3<>(2L, "Euro", "2021-07-25 00:00:02"));
        orderData.add(new Tuple3<>(50L, "Yen", "2021-07-25 00:00:05"));
        orderData.add(new Tuple3<>(3L, "Euro", "2021-07-25 00:00:07"));
        orderData.add(new Tuple3<>(5L, "Euro", "2021-07-25 00:00:10"));

        DataStream<Tuple3<Long, String, String>> orderStream = env.fromCollection(orderData);

        Table orderTable = tEnv.fromDataStream(orderStream, Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                .watermark("rowtime", "rowtime - interval '5' SECOND")
                .build());

        tEnv.createTemporaryView("Orders", orderTable);
        orderTable.printSchema();//only for debug
        tEnv.sqlQuery("SELECT * FROM Orders ORDER BY rowtime").execute().print();

    }
}
