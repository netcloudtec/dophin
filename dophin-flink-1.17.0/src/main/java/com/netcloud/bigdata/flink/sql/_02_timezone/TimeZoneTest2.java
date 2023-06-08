package com.netcloud.bigdata.flink.sql._02_timezone;

import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-17 14:01:44
 * @email netcloudtec@163.com
 * @description
 * 1、指定时间属性的两种方式：
 * 1) CREATE TABLE DDL创建表的时候指定
 * 2) 在DataStream中指定，在后续DataStream转Table中使用
 *
 * 2、事件时间在DataStream中指定示例(事件时间为字符串情况)
 * 1) 将DataStream转为Table时，使用fromDataStream(DataStream<T> var1, Schema var2);
 *    fromDataStream(DataStream<T> var1, Expression... var2)已经在Flink1.13版本以后被废弃
 * 2) 将DataStream转Table时，需要将字符串事件时间扩展新列并转为TIMESTAMP(3)或TIMESTAMP_LTZ(3)类型
 *    注意:yyyy-MM-dd HH:mm:ss和yyyy-MM-dd hh:mm:ss区别
 * 3) 设置watermark，对应列为扩展新列
 *
 * 3、事件时间由字符串转为TIMESTAMP(3)或TIMESTAMP_LTZ(3)在不同时区情况下分析
 * 1）事件时间类型为TIMESTAMP(3)默认时区为东八区、当时区设置为UTC，日期不会改变。
 * 2) 事件时间类型为TIMESTAMP_LTZ(3)默认时区为东八区、当时区设置为UTC，日期不好改变。
 * 3) 事件时间由字符串转为TIMESTAMP(3)或TIMESTAMP_LTZ(3)时候，建议使用默认分区(东八区)
 *
 */
public class TimeZoneTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //tableEnv.getConfig().getConfiguration().setString("table.local-time-zone", "GMT+08:00");
        //tableEnv.getConfig().getConfiguration().setString("table.local-time-zone", "UTC");
        //tableEnv.getConfig().getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");

        DataStream<Event> tuple3DataStream = env.fromCollection(Arrays.asList(
                new Event("2", 1L, "2021-07-25 00:00:00"),
                new Event("2", 101L, "2021-07-25 02:59:00"),
                new Event("2", 201L, "2021-07-25 07:00:00"),
                new Event("2", 301L, "2021-07-25 07:59:00"),
                new Event("2", 301L, "2021-07-25 08:00:00"),
                new Event("2", 301L, "2021-07-25 08:59:00"),
                new Event("2", 301L, "2021-07-26 00:00:00"),
                new Event("2", 301L, "2021-07-26 07:59:00"),
                new Event("2", 1L, "2021-07-26 08:00:00"),
                new Event("2", 301L, "2021-07-26 23:59:00")));

        Table table = tableEnv.fromDataStream(tuple3DataStream,
                Schema.newBuilder()
                        .column("status", "STRING")
                        .column("id", "BIGINT")
                        .column("timestamp", "STRING")
                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP_LTZ(3))")
                        .watermark("rowtime", "rowtime - interval '5' SECOND ")
                        .build());
        table.printSchema();

        tableEnv.createTemporaryView("source_db.source_table", table);
        //tableEnv.sqlQuery("select * from source_db.source_table").execute().print();

        tableEnv.sqlQuery("select cast(tumble_start(rowtime, INTERVAL '1' DAY) as string) AS tumble_start,cast(tumble_end(rowtime, INTERVAL '1' DAY) as string) AS tumble_end,count(1) as cnt from source_db.source_table GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY)").execute().print();

        /**
         * 也可以将Table转为DataStream输出打印
         Table result = tableEnv.sqlQuery("select cast(tumble_start(rowtime, INTERVAL '1' DAY) as string) AS tumble_start,cast(tumble_end(rowtime, INTERVAL '1' DAY) as string) AS tumble_end,count(1) as cnt from source_db.source_table GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY)");
         tableEnv.toDataStream(result).print();
         try {
         env.execute();
         } catch (Exception e) {
         e.printStackTrace();
         }
         */
    }

    @Data
    public static class Event {
        public String status;
        public Long id;
        public String timestamp;
        public Event() {

        }
        public Event(String status, Long id, String timestamp) {
            this.status = status;
            this.id = id;
            this.timestamp = timestamp;
        }
        @Override
        public String toString() {
            return "Event{" +
                    "status='" + status + '\'' +
                    ", id='" + id + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
