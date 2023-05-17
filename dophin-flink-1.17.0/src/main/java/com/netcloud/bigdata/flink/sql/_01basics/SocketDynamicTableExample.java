package com.netcloud.bigdata.flink.sql._01basics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * --hostname 127.0.0.1 --port 9999
 * <pre>
 *     nc -lk 9999
 *     Mary,12:00:00,./home
 *     Bob,12:00:00,./cart
 *     Mary,12:00:05,./prod?id=1
 *     Liz,12:01:00,./home
 *     Bob,12:01:30,./prod?id=3
 *     Mary,12:01:45,./prod?id=7
 *
 *     Mary,12:00:00,./home
 *     Bob,12:00:00,./cart
 *     Mary,12:02:00,./prod?id=1
 *     Mary,12:55:00,./prod?id=4
 *     Bob,13:01:00,./prod?id=5
 *     Liz,13:30:00,./home
 *     Liz,13:59:00,./prod?id=7
 *     Mary,14:00:00,./cart
 *     Liz,14:02:00,./home
 *     Bob,14:30:00,./prod?id=3
 *     Bob,14:40:00,./home
 * </pre>
 * @author netcloud
 * @date 2023-05-15 17:48:23
 * @email netcloudtec@163.com
 * @description
 */
public class SocketDynamicTableExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 9999);
        DataStream<String> inputDataStream = env.socketTextStream(hostname, port);
        DataStream<Click> mapDS = inputDataStream.map(new MapFunction<String, Click>() {
            @Override
            public Click map(String content) throws Exception {
                String[] strArr = content.split(",");
                return new Click(strArr[0], strArr[1], strArr[2]);
            }
        });

        //Table clickTable = tableEnv.fromDataStream(mapDS);
        tableEnv.createTemporaryView("clicks",mapDS);
        Table table = tableEnv.sqlQuery("select userName,count(url) as cnt from clicks group by userName");
        tableEnv.toChangelogStream(table).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Simple POJO.
     */
    public static class Click {
        public String userName;
        public String cTime;
        public String url;

        // for POJO detection in DataStream API
        public Click() {
        }

        // for structured type detection in Table API
        public Click(String userName, String cTime, String url) {
            this.userName = userName;
            this.cTime = cTime;
            this.url = url;
        }

        @Override
        public String toString() {
            return "Click{" +
                    "userName='" + userName + '\'' +
                    ", cTime='" + cTime + '\'' +
                    ", url='" + url + '\'' +
                    '}';
        }
    }
}
