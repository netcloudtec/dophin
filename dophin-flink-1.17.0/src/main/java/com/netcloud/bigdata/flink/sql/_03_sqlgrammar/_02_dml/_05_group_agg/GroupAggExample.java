package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._05_group_agg;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-05-24 11:13:34
 * @email netcloudtec@163.com
 * @description
 * Group聚合
 */
public class GroupAggExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String sourceSql = "CREATE TABLE source_table (\n"
                + "    order_id STRING,\n"
                + "    price BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.order_id.length' = '1',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '1000000'\n"
                + ")";

        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    order_id STRING,\n"
                + "    count_result BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String selectWhereSql = "insert into sink_table\n"
                + "  select order_id,\n"
                + "         count(*) as count_result\n"
                + "  from source_table\n"
                + "  group by order_id\n";

        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "GROUP AGG 案例");

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(selectWhereSql);
    }
}
