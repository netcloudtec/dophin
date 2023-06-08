package com.netcloud.bigdata.flink.sql._03_sqlgrammar._01_ddl;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-05-19 11:11:36
 * @email netcloudtec@163.com
 * @description
 * 1、FlinkSQL支持下列的CREATE子句
 * CREATE [TEMPORARY]TABLE
 * CREATE [TEMPORARY]VIEW
 * CREATE DATABASE
 * CREATE FUNCTION
 * 2、使用CREATE [TEMPORARY]TABLE WITH子句创建表
 *    使用CREATE [TEMPORARY]TABLE LIKE子句创建表是对已有表的扩展，很少使用
 *
 */
public class CreateTableExample {
    public static void main(String[] args) {
        final EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        /**
         * 创建表(CREATE TABLE WITH)并指定WATERMARK
         * 这里我们使用的connector是datagen,可自行选择其他
         */
        String sourceSql = "CREATE TABLE source_table (\n"
                + "    dim STRING,\n"
                + "    user_id BIGINT,\n"
                + "    price BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_ltz(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10000',\n"
                + "  'fields.dim.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '100000',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '100000'\n"
                + ")";

        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    dim STRING,\n"
                + "    user_id BIGINT,\n"
                + "    price BIGINT,\n"
                + "    row_time timestamp(3)\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String selectWhereSql = "insert into sink_table\n"
                + "SELECT dim,\n"
                + "\t   user_id,\n"
                + "\t   price,\n"
                + "\t   row_time\n"
                + "FROM source_table\n"
                ;
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(selectWhereSql);
    }
}
