package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._04_window_agg;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-05-19 17:37:21
 * @email netcloudtec@163.com
 * @description
 * 这里我们介绍渐进式窗口 Hop Windows
 * 应用场景: 周期内累计PV、UV指标（如:每天累计到当前这一分钟的PV、UV）
 */
public class CumulateWindowExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String exampleSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n"
                + "    money BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1000',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    window_end bigint,\n"
                + "    window_start timestamp(3),\n"
                + "    sum_money BIGINT,\n"
                + "    count_distinct_id BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "insert into sink_table\n"
                + "with tmp as (\n"
                + "    SELECT \n"
                + "      window_time as r_time,\n"
                + "      sum(money) as sum_money,\n"
                + "      count(distinct id) as count_distinct_id\n"
                + "    FROM TABLE(CUMULATE(\n"
                + "             TABLE source_table\n"
                + "             , DESCRIPTOR(row_time)\n"
                + "             , INTERVAL '60' SECOND\n"
                + "             , INTERVAL '1' DAY))\n"
                + "    GROUP BY window_start, \n"
                + "            window_end,\n"
                + "            window_time,\n"
                + "            mod(id, 1000)\n"
                + ")\n"
                + "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(sum_money) as sum_money,\n"
                + "      sum(count_distinct_id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE tmp\n"
                + "         , DESCRIPTOR(r_time)\n"
                + "         , INTERVAL '60' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";
        for (String innerSql : exampleSql.split(";")) {
            tableEnv.executeSql(innerSql);
        }
    }
}
