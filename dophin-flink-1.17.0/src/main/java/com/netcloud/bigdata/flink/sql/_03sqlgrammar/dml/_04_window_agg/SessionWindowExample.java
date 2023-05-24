package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._04_window_agg;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-19 16:46:56
 * @email netcloudtec@163.com
 * @description
 * 这里我们介绍session窗口 Session Windows
 */
public class SessionWindowExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String sql = "-- 数据源表，用户购买行为记录表\n"
                + "CREATE TABLE source_table (\n"
                + "    -- 维度数据\n"
                + "    dim STRING,\n"
                + "    -- 用户 id\n"
                + "    user_id BIGINT,\n"
                + "    -- 用户\n"
                + "    price BIGINT,\n"
                + "    -- 事件时间戳\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    -- watermark 设置\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '5',\n"
                + "  'fields.dim.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '100000',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '100000'\n"
                + ");\n"
                + "\n"
                + "-- 数据汇表\n"
                + "CREATE TABLE sink_table (\n"
                + "    dim STRING,\n"
                + "    pv BIGINT,\n"
                + "    window_start BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "-- 数据处理逻辑\n"
                + "insert into sink_table\n"
                + "select dim,\n"
                + "\t     COUNT(1) as pv,\n"
                + "\t\t   UNIX_TIMESTAMP(CAST(session_start(row_time, interval '1' second) AS STRING)) * 1000 as window_start\n"
                + "\t FROM source_table\n"
                + "\t GROUP BY dim,\n"
                + "            session(row_time, interval '5' minute)\n";
        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
