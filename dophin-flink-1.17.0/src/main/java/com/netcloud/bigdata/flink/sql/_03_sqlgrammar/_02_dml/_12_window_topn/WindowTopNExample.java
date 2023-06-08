package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._12_window_topn;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-06-02 11:12:09
 * @email netcloudtec@163.com
 * @description
 * WinddowTopN: 每个组中取TopN
 */
public class WindowTopNExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql = "CREATE TABLE source_table (\n"
                + "    name BIGINT NOT NULL,\n"
                + "    search_cnt BIGINT NOT NULL,\n"
                + "    key BIGINT NOT NULL,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.name.min' = '1',\n"
                + "  'fields.name.max' = '10',\n"
                + "  'fields.key.min' = '1',\n"
                + "  'fields.key.max' = '2',\n"
                + "  'fields.search_cnt.min' = '1000',\n"
                + "  'fields.search_cnt.max' = '10000'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    key BIGINT,\n"
                + "    name BIGINT,\n"
                + "    search_cnt BIGINT,\n"
                + "    window_start TIMESTAMP(3),\n"
                + "    window_end TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT key, name, search_cnt, window_start, window_end\n"
                + "FROM (\n"
                + "   SELECT key, name, search_cnt, window_start, window_end, \n"
                + "     ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key\n"
                + "       ORDER BY search_cnt desc) AS rownum\n"
                + "   FROM (\n"
                + "      SELECT window_start, window_end, key, name, search_cnt as search_cnt\n"
                + "      FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '1' MINUTES))\n"
                + "      GROUP BY window_start, window_end, key, name,search_cnt\n"
                + "   )\n"
                + ")\n"
                + "WHERE rownum <= 100\n";

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
