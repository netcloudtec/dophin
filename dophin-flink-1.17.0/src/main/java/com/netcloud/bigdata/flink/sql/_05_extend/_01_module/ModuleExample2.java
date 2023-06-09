package com.netcloud.bigdata.flink.sql._05_extend._01_module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * @author netcloud
 * @date 2023-06-09 16:56:13
 * @email netcloudtec@163.com
 * @description 使用Java API加载、卸载、使用、列出Module
 */
public class ModuleExample2 {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Show initially loaded and enabled modules
        tableEnv.listModules();
        // +-------------+
        // | module name |
        // +-------------+
        // |        core |
        // +-------------+
        tableEnv.listFullModules();
        // +-------------+------+
        // | module name | used |
        // +-------------+------+
        // |        core | true |
        // +-------------+------+

        // Load a hive module
        tableEnv.loadModule("hive", new HiveModule());

        // Show all enabled modules
        tableEnv.listModules();
        // +-------------+
        // | module name |
        // +-------------+
        // |        core |
        // |        hive |
        // +-------------+

        // Show all loaded modules with both name and use status
        tableEnv.listFullModules();
        // +-------------+------+
        // | module name | used |
        // +-------------+------+
        // |        core | true |
        // |        hive | true |
        // +-------------+------+

        // Change resolution order
        tableEnv.useModules("hive", "core");
        tableEnv.listModules();
        // +-------------+
        // | module name |
        // +-------------+
        // |        hive |
        // |        core |
        // +-------------+
        tableEnv.listFullModules();
        // +-------------+------+
        // | module name | used |
        // +-------------+------+
        // |        hive | true |
        // |        core | true |
        // +-------------+------+

        // Disable core module
        tableEnv.useModules("hive");
        tableEnv.listModules();
        // +-------------+
        // | module name |
        // +-------------+
        // |        hive |
        // +-------------+
        tableEnv.listFullModules();
        // +-------------+-------+
        // | module name |  used |
        // +-------------+-------+
        // |        hive |  true |
        // |        core | false |
        // +-------------+-------+

        // Unload hive module
        tableEnv.unloadModule("hive");
        tableEnv.listModules();
        // Empty set
        tableEnv.listFullModules();
        // +-------------+-------+
        // | module name |  used |
        // +-------------+-------+
        // |        hive | false |
        // +-------------+-------+
    }
}
