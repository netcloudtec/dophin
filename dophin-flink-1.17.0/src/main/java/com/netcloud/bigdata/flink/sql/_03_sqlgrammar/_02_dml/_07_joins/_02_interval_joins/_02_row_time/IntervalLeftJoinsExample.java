package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._02_interval_joins._02_row_time;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-25 14:50:36
 * @email netcloudtec@163.com
 * @description
 */
public class IntervalLeftJoinsExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql = "CREATE TABLE show_log (\n"
                + "    log_id BIGINT,\n"
                + "    show_params STRING,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.show_params.length' = '1',\n"
                + "  'fields.log_id.min' = '5',\n"
                + "  'fields.log_id.max' = '15'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE click_log (\n"
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
                + "    show_log.log_id as s_id,\n"
                + "    show_log.show_params as s_params,\n"
                + "    click_log.log_id as c_id,\n"
                + "    click_log.click_params as c_params\n"
                + "FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id\n"
                + "AND show_log.row_time BETWEEN click_log.row_time - INTERVAL '5' SECOND AND click_log.row_time + INTERVAL '5' SECOND;";

        /**
         * join 算子：{@link org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay}
         *                 -> {@link org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin}
         *                       -> {@link org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction}
         */

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
