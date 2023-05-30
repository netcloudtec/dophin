package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._07_joins._02_interval_joins._02_row_time;

import com.netcloud.bigdata.flink.sql.bean.ClickLog;
import com.netcloud.bigdata.flink.sql.bean.ShowLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-05-25 14:54:06
 * @email netcloudtec@163.com
 * @description
 * nc -lk 9999
 * -- hostname1 127.0.0.1 --port1  9999
 * 1,logParams1,2023-05-25 15:00:00
 *
 * nc -lk 9998
 * -- hostname1 127.0.0.1 --port1  9998
 * 1,clickLog12,2023-05-25 12:00:00
 * 1,clickLog12,2023-05-25 15:00:00
 * 1,clickLog12,2023-05-25 19:00:00
 * 1,clickLog12,2023-05-25 20:00:00
 */
public class SocketStreamSQLJoinsTest {
    public static void main(String[] args) {
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
        Table showLogTable = tableEnv.fromDataStream(showLogDataStream, Schema.newBuilder()
                .column("logId", "STRING")
                .column("showParams", "STRING")
                .column("eventTime", "STRING")
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`eventTime`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime - interval '5' SECOND ")
                .build());

        Table clickLogTable = tableEnv.fromDataStream(clickLogDataStream, Schema.newBuilder()
                .column("logId", "STRING")
                .column("clickParams", "STRING")
                .column("eventTime", "STRING")
                .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`eventTime`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime - interval '5' SECOND ")
                .build());

        // 命名列
        final Table showLogTableNamedColumn =
                showLogTable.as(
                        "log_id",
                        "show_params",
                        "event_time",
                        "row_time"
                );

        // 命名列
        final Table clickLogTableNamedColumn =
                clickLogTable.as(
                        "log_id",
                        "click_params",
                        "event_time",
                        "row_time"
                );

        tableEnv.createTemporaryView("show_log_table", showLogTableNamedColumn);
        tableEnv.createTemporaryView("click_log_table",clickLogTableNamedColumn);

        String sql="SELECT\n"
                + "    show_log_table.log_id as s_id,\n"
                + "    show_log_table.show_params as s_params,\n"
                + "    click_log_table.log_id as c_id,\n"
                + "    click_log_table.click_params as c_params\n"
                + "FROM show_log_table INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id\n"
                + "AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '4' HOUR AND click_log_table.row_time;";

        // 可更改为LEFT JOIN、RIGHT JOIN、FULL JOIN进行验证
        tableEnv.sqlQuery(sql).execute().print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
