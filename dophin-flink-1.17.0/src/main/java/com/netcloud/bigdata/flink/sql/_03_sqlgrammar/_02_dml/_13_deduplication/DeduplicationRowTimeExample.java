package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._13_deduplication;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-02 14:15:46
 * @email netcloudtec@163.com
 * @description
 * 使用ROW_NUMBER() 的rom_number=1 实现去重排序字段一定是时间属性
 */
public class DeduplicationRowTimeExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql = "CREATE TABLE source_table (\n"
                + "    user_id BIGINT COMMENT '用户 id',\n"
                + "    level STRING COMMENT '用户等级',\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)) COMMENT '事件时间戳',\n"
                + "    WATERMARK FOR row_time AS row_time\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.level.length' = '1',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '1000000'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    level STRING COMMENT '等级',\n"
                + "    uv BIGINT COMMENT '当前等级用户数',\n"
                + "    row_time timestamp(3) COMMENT '时间戳'\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "select \n"
                + "    level\n"
                + "    , count(1) as uv\n"
                + "    , max(row_time) as row_time\n"
                + "from (\n"
                + "      SELECT\n"
                + "          user_id,\n"
                + "          level,\n"
                + "          row_time,\n"
                + "          row_number() over(partition by user_id order by row_time desc) as rn\n"
                + "      FROM source_table\n"
                + ")\n"
                + "where rn = 1\n"
                + "group by \n"
                + "    level";

        for (String innerSql : sql.split(";")) {
            tableEnv.executeSql(innerSql);
        }
    }
}
