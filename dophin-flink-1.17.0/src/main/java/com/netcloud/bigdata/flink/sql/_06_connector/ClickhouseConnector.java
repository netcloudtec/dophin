package com.netcloud.bigdata.flink.sql._06_connector;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author netcloud
 * @date 2023-06-13 11:34:29
 * @email netcloudtec@163.com
 * @description
 * 使用自定义第三方的connector https://github.com/itinycheng/flink-connector-clickhouse.git
 * 自编译打包的时候修改flink版本
 * nc -lk 9999
 * 2,1,2023-05-25 00:00:00
 * 2,101,2023-05-25 02:59:00
 * 2,201,2023-05-25 07:00:00
 * 2,301,2023-05-25 07:59:00
 * 2,301,2023-05-25 08:00:00
 * 2,301,2023-05-25 08:59:00
 * 2,301,2023-05-26 00:00:00
 * 2,301,2023-05-26 07:59:00
 * 2,1,2023-05-26 08:00:00
 * 2,301,2023-05-26 23:59:00
 *
 */
public class ClickhouseConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 9999);
        DataStream<String> inputDataStream = env.socketTextStream(hostname, port);

        DataStream<Event> mapStream = inputDataStream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String content) throws Exception {
                String[] split = content.split(",");
                return new Event(split[0], split[1], split[2]);
            }
        });

        Table table = tableEnv.fromDataStream(mapStream,
                Schema.newBuilder()
                        .column("status", "STRING")
                        .column("eventId", "STRING")
                        .column("eventTime", "STRING")
//                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`eventTime`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP_LTZ(3))")
//                        .watermark("rowtime", "rowtime - interval '5' SECOND ")
                        .build());

        tableEnv.createTemporaryView("source_db.source_table", table);

        String sinkTableSQL = "CREATE TABLE sink_table (\n" +
                " status String,\n" +
                " event_id String,\n" +
                " event_time String\n" +
                ") WITH (\n" +
                "  'connector' = 'clickhouse',\n" +
                "  'url' = 'clickhouse://10.2.9.124:8123',\n" +
                "  'database-name' = 'default',\n" +
                "  'table-name' = 'event_test',\n" +
                "  'sink.batch-size' = '500', \n" +
                "  'sink.flush-interval' = '100',\n" +
                "  'sink.max-retries' = '3'\n" +
                ")\n";

        String result = "INSERT INTO sink_table SELECT status ,eventId, eventTime FROM source_db.source_table ";
        tableEnv.executeSql(sinkTableSQL);
        tableEnv.executeSql(result);

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
        public String eventId;
        public String eventTime;
        public Event() {

        }
        public Event(String status, String eventId, String eventTime) {
            this.status = status;
            this.eventId = eventId;
            this.eventTime = eventTime;
        }
        @Override
        public String toString() {
            return "Event{" +
                    "status='" + status + '\'' +
                    ", eventId='" + eventId + '\'' +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }
}
