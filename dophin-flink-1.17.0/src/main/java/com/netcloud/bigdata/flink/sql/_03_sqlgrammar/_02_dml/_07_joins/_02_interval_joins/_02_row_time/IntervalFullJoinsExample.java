package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._02_interval_joins._02_row_time;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-25 14:51:32
 * @email netcloudtec@163.com
 * @description
 * 曝光日志关联点击日志筛选出既有曝光又有点击的数据，条件是曝光关联之后发生4小时之内的点击。
 * 如:曝光日志 1,logParams1,2023-05-25 15:00:00 只能关联点击日志时间为 2023-05-25 15:00:00 - 2023-05-25 19:00:00范围的
 *
 */
public class IntervalFullJoinsExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql =
                "CREATE TABLE show_log_table (\n"
                        + "    log_id BIGINT,\n"
                        + "    show_params STRING,\n"
                        + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                        + "    WATERMARK FOR row_time AS row_time\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'fields.show_params.length' = '1',\n"
                        + "  'fields.log_id.min' = '1',\n"
                        + "  'fields.log_id.max' = '10'\n"
                        + ");\n"
                        + "\n"
                        + "CREATE TABLE click_log_table (\n"
                        + "    log_id BIGINT,\n"
                        + "    click_params STRING,\n"
                        + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                        + "    WATERMARK FOR row_time AS row_time\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'fields.click_params.length' = '1',\n"
                        + "  'fields.log_id.min' = '1',\n"
                        + "  'fields.log_id.max' = '10'\n"
                        + ");\n"
                        + "\n"
                        + "CREATE TABLE sink_table (\n"
                        + "    s_id BIGINT,\n"
                        + "    s_params STRING,\n"
                        + "    c_id BIGINT,\n"
                        + "    c_params STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");\n"
                        + "\n"
                        + "INSERT INTO sink_table\n"
                        + "SELECT\n"
                        + "    show_log_table.log_id as s_id,\n"
                        + "    show_log_table.show_params as s_params,\n"
                        + "    click_log_table.log_id as c_id,\n"
                        + "    click_log_table.click_params as c_params\n"
                        + "FROM show_log_table FULL JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id\n"
                        + "AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '5' SECOND AND click_log_table.row_time;";

        /**
         * join 算子：{@link org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay}
         *                 -> {@link org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin}
         *                       -> {@link org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction}
         */

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
