package com.netcloud.bigdata.flink.sql._02timezone;

import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author netcloud
 * @date 2023-05-17 10:33:58
 * @email netcloudtec@163.com
 * @description
 */
public class TimeZoneTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.getConfig().getConfiguration().setString("table.local-time-zone", "GMT+08:00");
//        tableEnv.getConfig().getConfiguration().setString("table.local-time-zone", "UTC");
//        tableEnv.getConfig().getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");


        DataStream<Event> tuple3DataStream = env.fromCollection(Arrays.asList(
                new Event("2", 1L, 1627218000000L + 5000L),
                new Event("2", 101L, 1627218000000L + 6000L),
                new Event("2", 201L, 1627218000000L + 7000L),
                new Event("2", 301L, 1627218000000L + 7000L),
                new Event("2", 301L, 1627218000000L + 7000L),
                new Event("2", 301L, 1627218000000L + 7000L),
                new Event("2", 301L, 1627218000000L + 7000L),
                new Event("2", 301L, 1627218000000L + 7000L),
                new Event("2", 1L, 1627254000000L), // 北京时间：2021-07-26 07:00:00
                new Event("2", 301L, 1627218000000L + 86400000 + 7000L)));
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
//                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                );

        Table table = tableEnv.fromDataStream(tuple3DataStream,
                Schema.newBuilder()
                        .column("status", "STRING")
                        .column("id", "BIGINT")
                        .column("timestamp", "BIGINT")
                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`/1000)) AS TIMESTAMP(3))")
                        .watermark("rowtime", "rowtime - interval '5' SECOND ")
                        .build());
        table.printSchema();

        tableEnv.createTemporaryView("source_db.source_table", table);
        tableEnv.sqlQuery("select * from source_db.source_table").execute().print();

//        tableEnv.sqlQuery("select cast(tumble_start(rowtime, INTERVAL '1' DAY) as string),cast(tumble_end(rowtime, INTERVAL '1' DAY) as string),count(1) as cnt from source_db.source_table GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY)").execute().print();
//        String sql = "SELECT count(1) FROM source_db.source_table GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY)";
//        tableEnv.sqlQuery(sql).execute().print();

    }

    @Data
    public static class Event {
        public String status;
        public Long id;
        public Long timestamp;

        public Event() {

        }

        public Event(String status, Long id, Long timestamp) {
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
