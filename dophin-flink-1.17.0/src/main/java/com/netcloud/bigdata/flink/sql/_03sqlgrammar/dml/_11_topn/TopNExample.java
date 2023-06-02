package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._11_topn;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-31 17:59:36
 * @email netcloudtec@163.com
 * @description
 * FlinkSQL TopN
 * 场景:统计关键词点击率前100名
 * 注意：
 * 这里为什么数据会有撤回流，是因为在整个流式计算中，数据源源不断的到来，而我们并没有划定窗口的界限，
 * 因此相当于flink缓存了中间所有数据的状态，然后每次都要筛选一遍，得出结果，这对于大状态很容易有问题！
 */
public class TopNExample {
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
                + "    `timestamp` TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT key, name, search_cnt, row_time as `timestamp`\n"
                + "FROM (\n"
                + "   SELECT key, name, search_cnt, row_time, \n"
                + "     ROW_NUMBER() OVER (PARTITION BY key\n"
                + "       ORDER BY search_cnt desc) AS rownum\n"
                + "   FROM source_table)\n"
                + "WHERE rownum <= 100\n";

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
