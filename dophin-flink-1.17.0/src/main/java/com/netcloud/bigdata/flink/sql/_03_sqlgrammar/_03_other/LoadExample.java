package com.netcloud.bigdata.flink.sql._03_sqlgrammar._03_other;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-08 14:33:59
 * @email netcloudtec@163.com
 * @description
 * LOAD 语句用于加载内置的或用户自定义的模块。
 * UNLOAD 语句用于卸载内置的或用户自定义的模块。
 * 可以使用 TableEnvironment 的 executeSql() 方法执行 LOAD 语句。如果 LOAD 操作执行成功，executeSql() 方法会返回 ‘OK’，否则会抛出异常。
 * LOAD MODULE module_name [WITH ('key1' = 'val1', 'key2' = 'val2', ...)]
 * UNLOAD MODULE module_name
 */
public class LoadExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 加载 hive 模块
        tEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '3.1.3')");
        tEnv.executeSql("SHOW MODULES").print();

        // 卸载 core 模块
        tEnv.executeSql("UNLOAD MODULE core");
        tEnv.executeSql("SHOW MODULES").print();

    }
}
