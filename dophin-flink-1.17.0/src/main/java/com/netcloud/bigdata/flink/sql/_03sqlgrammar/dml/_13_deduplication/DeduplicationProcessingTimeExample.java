package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._13_deduplication;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-02 14:21:40
 * @email netcloudtec@163.com
 * @description
 * 使用ROW_NUMBER() 的rom_number=1 实现去重排序字段一定是时间属性
 */
public class DeduplicationProcessingTimeExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql = "CREATE TABLE source_table (\n"
                + "    user_id BIGINT COMMENT '用户 id',\n"
                + "    name STRING COMMENT '用户姓名',\n"
                + "    server_timestamp BIGINT COMMENT '用户访问时间戳',\n"
                + "    proctime AS PROCTIME()\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.name.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '10',\n"
                + "  'fields.server_timestamp.min' = '1',\n"
                + "  'fields.server_timestamp.max' = '100000'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT,\n"
                + "    name STRING,\n"
                + "    server_timestamp BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "select user_id,\n"
                + "       name,\n"
                + "       server_timestamp\n"
                + "from (\n"
                + "      SELECT\n"
                + "          user_id,\n"
                + "          name,\n"
                + "          server_timestamp,\n"
                + "          row_number() over(partition by user_id order by proctime) as rn\n"
                + "      FROM source_table\n"
                + ")\n"
                + "where rn = 1";

        for (String innerSql : sql.split(";")) {
            tableEnv.executeSql(innerSql);
        }
    }
}
