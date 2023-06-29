package com.netcloud.bigdata.flink.sql._01_basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-20 10:20:45
 * @email netcloudtec@163.com
 * @description 一个SQL查询案例
 */
public class BasicSQLQueryExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1. 创建一个数据源（输入）表，这里的数据源是 flink 自带的一个随机 mock 数据的数据源。
        String sourceSql = "CREATE TABLE source_table (\n"
                + "    sku_id STRING,\n"
                + "    price BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.sku_id.length' = '1',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '1000000'\n"
                + ")";
        // 2. 创建一个数据汇（输出）表，打印到 console 中
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    sku_id STRING,\n"
                + "    count_result BIGINT,\n"
                + "    sum_result BIGINT,\n"
                + "    avg_result DOUBLE,\n"
                + "    min_result BIGINT,\n"
                + "    max_result BIGINT,\n"
                + "    PRIMARY KEY (`sku_id`) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";
        // 3. 执行一段 group by 的聚合 SQL 查询
        String selectWhereSql = "insert into sink_table\n"
                + "select sku_id,\n"
                + "       count(*) as count_result,\n"
                + "       sum(price) as sum_result,\n"
                + "       avg(price) as avg_result,\n"
                + "       min(price) as min_result,\n"
                + "       max(price) as max_result\n"
                + "from source_table\n"
                + "group by sku_id";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(selectWhereSql);
    }
}
