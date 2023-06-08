package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._02_interval_joins._01_proctime;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-25 17:06:30
 * @email netcloudtec@163.com
 * @description
 */
public class IntervalInnerJoinsProcesingTime {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql = "CREATE TABLE source_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    proctime AS PROCTIME()\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.name.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '100000'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE dim_table (\n"
                + "  user_id BIGINT,\n"
                + "  platform STRING,\n"
                + "  proctime AS PROCTIME()\n"
                + ")\n"
                + "WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.platform.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '100000'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    platform STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT\n"
                + "    s.user_id as user_id,\n"
                + "    s.name as name,\n"
                + "    d.platform as platform\n"
                + "FROM source_table s, dim_table as d\n"
                + "WHERE s.user_id = d.user_id\n"
                + "AND s.proctime BETWEEN d.proctime - INTERVAL '4' HOUR AND d.proctime;";

        String exampleSql = "CREATE TABLE show_log_table (\n"
                + "    log_id BIGINT,\n"
                + "    show_params STRING,\n"
                + "    proctime AS PROCTIME()\n"
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
                + "    proctime AS PROCTIME()\n"
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
                + "FROM show_log_table INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id\n"
                + "AND show_log_table.proctime BETWEEN click_log_table.proctime - INTERVAL '4' HOUR AND click_log_table.proctime;";

        Arrays.stream(exampleSql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
