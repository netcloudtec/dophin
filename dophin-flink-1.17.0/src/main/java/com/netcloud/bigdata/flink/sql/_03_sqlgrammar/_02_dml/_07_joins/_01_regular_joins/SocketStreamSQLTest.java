package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._01_regular_joins;

import com.netcloud.bigdata.flink.sql.bean.ClickLog;
import com.netcloud.bigdata.flink.sql.bean.ShowLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author netcloud
 * @date 2023-05-24 15:57:05
 * @email netcloudtec@163.com
 * @description
 * nc -lk 9999
 * -- hostname1 127.0.0.1 --port1  9999
 * 1, logParams1,2023-05-25 15:00:00
 * 2,logParams2,2023-05-25 15:00:00
 * 3,logParams3,2023-05-25 15:00:00
 *
 * nc -lk 9998
 * -- hostname1 127.0.0.1 --port1  9998
 * 1,clickLog1,2023-05-25 15:00:00
 * 2,clickLog2,2023-05-25 15:00:00
 * 3,clickLog3,2023-05-25 15:00:00
 */
public class SocketStreamSQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname1 = params.get("hostname1", "localhost");
        int port1 = params.getInt("port1", 9999);
        DataStream<String> inputDataStream1 = env.socketTextStream(hostname1, port1);

        String hostname2 = params.get("hostname2", "localhost");
        int port2 = params.getInt("port2", 9998);
        DataStream<String> inputDataStream2 = env.socketTextStream(hostname2, port2);

        DataStream<ShowLog> showLogDataStream = inputDataStream1.map(new MapFunction<String, ShowLog>() {
            @Override
            public ShowLog map(String content) throws Exception {
                String[] split = content.split(",");
                return new ShowLog(split[0], split[1],split[2]);
            }
        });

        DataStream<ClickLog> clickLogDataStream = inputDataStream2.map(new MapFunction<String, ClickLog>() {
            @Override
            public ClickLog map(String content) throws Exception {
                String[] split = content.split(",");
                return new ClickLog(split[0], split[1],split[2]);
            }
        });
        //TODO 将DataStream转Table
        Table showLogTable = tableEnv.fromDataStream(showLogDataStream);
        Table clickLogTable = tableEnv.fromDataStream(clickLogDataStream);

        // 命名列
        final Table showLogTableNamedColumn =
                showLogTable.as(
                        "log_id",
                        "show_params");

        // 命名列
        final Table clickLogTableNamedColumn =
                clickLogTable.as(
                        "log_id",
                        "click_params");
        showLogTableNamedColumn.printSchema();
        tableEnv.createTemporaryView("show_log", showLogTableNamedColumn);
        tableEnv.createTemporaryView("click_log",clickLogTableNamedColumn);
        // 可更改为LEFT JOIN、RIGHT JOIN、FULL JOIN进行验证
        String selectSQL="SELECT show_log.log_id,show_log.show_params, click_log.log_id,click_log.click_params FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id";
        tableEnv.sqlQuery(selectSQL).execute().print();
        env.execute();
    }
}
