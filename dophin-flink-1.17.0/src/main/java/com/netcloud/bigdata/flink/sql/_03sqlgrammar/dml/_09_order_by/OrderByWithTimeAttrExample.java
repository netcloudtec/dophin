package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._09_order_by;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-31 17:03:47
 * @email netcloudtec@163.com
 * @description
 * 实时任务中，orderBy字句中必须要有时间属性字段，并且时间属性必须为升序时间属性
 * 即: WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND
 *    WATERMARK FOR rowtime AS rowtime
 * 功能:1) 对延迟的数据重新升序排序(前提是设置WATERMARK，否则延迟的数据会丢失，不参与排序)
 *     2) 实时任务用的比较少
 */
public class OrderByWithTimeAttrExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sql = "CREATE TABLE source_table_1 (\n"
                + "    user_id BIGINT NOT NULL,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '10'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT user_id\n"
                + "FROM source_table_1\n"
                + "Order By row_time, user_id desc\n";

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
