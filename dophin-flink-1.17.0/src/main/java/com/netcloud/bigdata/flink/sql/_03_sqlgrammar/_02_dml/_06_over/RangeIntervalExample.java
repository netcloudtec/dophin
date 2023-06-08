package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._06_over;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-24 11:44:06
 * @email netcloudtec@163.com
 * @description
 * Over Range
 */
public class RangeIntervalExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String sql = "CREATE TABLE source_table (\n"
                + "    order_id BIGINT,\n"
                + "    product BIGINT,\n"
                + "    amount BIGINT,\n"
                + "    order_time as PROCTIME()\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.order_id.min' = '1',\n"
                + "  'fields.order_id.max' = '2',\n"
                + "  'fields.amount.min' = '1',\n"
                + "  'fields.amount.max' = '10',\n"
                + "  'fields.product.min' = '1',\n"
                + "  'fields.product.max' = '2'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    product BIGINT,\n"
                + "    order_time TIMESTAMP(3),\n"
                + "    amount BIGINT,\n"
                + "    one_hour_prod_amount_sum BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT product, order_time, amount,\n"
                + "  SUM(amount) OVER (\n"
                + "    PARTITION BY product\n"
                + "    ORDER BY order_time\n"
                + "    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW\n"
                + "  ) AS one_hour_prod_amount_sum\n"
                + "FROM source_table";

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
