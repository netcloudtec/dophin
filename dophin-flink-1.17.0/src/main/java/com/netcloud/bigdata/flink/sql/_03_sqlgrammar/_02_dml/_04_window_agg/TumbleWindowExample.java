package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._04_window_agg;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-05-19 14:55:27
 * @email netcloudtec@163.com
 * @description
 * 1、FlinkSQL支持四种时间窗口
 * 滚动窗口Tumble Windows
 * 滑动窗口Hop Windows
 * Session窗口Session Windows
 * 渐进式窗口Cumulate Windows
 *2、这里我们介绍滚动窗口 Tumble Windows
 */
public class TumbleWindowExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sourceSql = "CREATE TABLE source_table (\n"
                + "    dim STRING,\n"
                + "    user_id BIGINT,\n"
                + "    price BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
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
                + "    pv BIGINT,\n"
                + "    sum_price BIGINT,\n"
                + "    max_price BIGINT,\n"
                + "    min_price BIGINT,\n"
                + "    uv BIGINT,\n"
                + "    window_start BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String selectWhereSql = "INSERT INTO sink_table\n"
                + "SELECT dim,\n"
                + "\t   COUNT(*) as pv,\n"
                + "\t   sum(price) as sum_price,\n"
                + "\t   max(price) as max_price,\n"
                + "\t   min(price) as min_price,\n"
                + "\t   COUNT(DISTINCT user_id) as uv,\n"
                + "\t   UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start \n"
                + "\t FROM TABLE(TUMBLE(\n"
                + "\t \t\t\tTABLE source_table\n"
                + "\t \t\t\t, DESCRIPTOR(row_time)\n"
                + "\t \t\t\t, INTERVAL '60' SECOND))\n"
                + "\t GROUP BY window_start, \n"
                + "\t  \t\t  window_end,\n"
                + "\t\t\t  dim\n";

        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "1.17.0 WINDOW TVF TUMBLE WINDOW 案例");

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(selectWhereSql);
    }
}
