package com.netcloud.bigdata.flink.sql._05_extend._01_module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-09 16:35:34
 * @email netcloudtec@163.com
 * @description
 * 使用SQL API加载、卸载、使用、列出Module
 */
public class ModuleExample {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //展示加载启用的module
        tableEnv.executeSql("SHOW MODULES").print();
        tableEnv.executeSql("SHOW FULL MODULES").print();

        //加载hive module
        tableEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '2.3.3')");
        //展示所有启用的module
        tableEnv.executeSql("SHOW MODULES").print();
        //展示所有加载的module以及他们的启用状态
        tableEnv.executeSql("SHOW FULL MODULES").print();

        // 改变 module的解析顺序
        tableEnv.executeSql("USE MODULES hive, core");
        tableEnv.executeSql("SHOW MODULES").print();

        // 禁用 core module
        tableEnv.executeSql("USE MODULES hive");
        tableEnv.executeSql("SHOW MODULES").print();

        // 卸载 hive module
        tableEnv.executeSql("UNLOAD MODULE hive");
        tableEnv.executeSql("SHOW FULL MODULES").print();
        /**
         * // +-------------+-------+
         * // | module name |  used |
         * // +-------------+-------+
         * // |        hive | false |
         * // +-------------+-------+
         */
    }
}
