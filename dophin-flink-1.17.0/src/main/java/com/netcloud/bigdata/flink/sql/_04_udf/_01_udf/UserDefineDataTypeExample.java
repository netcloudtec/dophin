package com.netcloud.bigdata.flink.sql._04_udf._01_udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-20 11:53:42
 * @email netcloudtec@163.com
 * @description 用户自定义数据类型使用示例
 */
public class UserDefineDataTypeExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册函数
        tableEnv.createTemporarySystemFunction("user_scalar_func", UserScalarFunction.class);

        // 创建数据源表
        String sourceTable = "CREATE TABLE source_table (\n" +
                "    user_id BIGINT NOT NULL COMMENT '用户 id'\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '10'\n" +
                ");";
        // 创建输出表
        String sinkTable = "CREATE TABLE sink_table (\n" +
                "    result_row_1 ROW<age INT, name STRING, totalBalance DECIMAL(10, 2)>,\n" +
                "    result_row_2 STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");";

        String executeSQL = "INSERT INTO sink_table\n" +
                "select\n" +
                "    -- 4.a. 用户自定义类型作为输出\n" +
                "    user_scalar_func(user_id) as result_row_1,\n" +
                "    -- 4.b. 用户自定义类型作为输出及输入\n" +
                "    user_scalar_func(user_scalar_func(user_id)) as result_row_2\n" +
                "from source_table;";
        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql(sinkTable);
        tableEnv.executeSql(executeSQL);
    }
}
