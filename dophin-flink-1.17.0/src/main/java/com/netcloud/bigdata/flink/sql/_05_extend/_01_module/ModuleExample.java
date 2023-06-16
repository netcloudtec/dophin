package com.netcloud.bigdata.flink.sql._05_extend._01_module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-09 16:35:34
 * @email netcloudtec@163.com
 * @description
 * 1、实时数仓建设往往是随着离线数仓而建设的。实时数据使用Flink产出、离线数据使用Hive/Spark产出
 * Module的使用可以在Flink中支持Hive UDF（拓展Flink UDF的能力、可插拔的）非常高效。
 * 1)用户可以自定义函数，将其加载进入Flink，在FlinkSQL和Table API中使用。
 * 2)加载官方提供的Hive Module，将Hive已有的内置函数作为Flink的内置函数使用。
 * 2、Flink包含三种Module
 * 1)CoreModule
 * 2)HiveModule
 * 3)用户自定义Module
 * 3、注意函数的加载顺序
 * 4、使用SQL API加载、卸载、使用、列出Module
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

        //加载hive module maven中必须引入flink-connector-hive_2.12、hive-exec
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
