package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._05_array_expansion;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-31 14:06:42
 * @email netcloudtec@163.com
 * @description
 */
public class ArrayExpansionExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE show_log_table (\n"
                + "    log_id BIGINT,\n"
                + "    show_params ARRAY<STRING>\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.log_id.min' = '1',\n"
                + "  'fields.log_id.max' = '10'\n"
                + ");\n"
                + "CREATE TABLE sink_table (\n"
                + "    log_id BIGINT,\n"
                + "    show_param STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT\n"
                + "    log_id,\n"
                + "    t.show_param as show_param\n"
                + "FROM show_log_table\n"
                + "CROSS JOIN UNNEST(show_params) AS t (show_param)";

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
