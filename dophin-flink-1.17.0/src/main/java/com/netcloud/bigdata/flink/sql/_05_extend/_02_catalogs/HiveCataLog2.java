package com.netcloud.bigdata.flink.sql._05_extend._03_catalogs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author netcloud
 * @date 2023-06-08 11:32:39
 * @email netcloudtec@163.com
 * @description
 * 操作CataLog API
 * 前提是CataLog、DataBase、Table已经被创建并注册成功
 */
public class HiveCataLog2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String name = "myhive";
        String defaultDatabase = "mydb";
        String hiveConfDir = "/Users/yangshaojun/Bigdata_AI/data_center_workspace/dophin/data/hive-conf";
        //如果默认库不存在会报错 Configured default database mydatabase doesn't exist in catalog myhive.
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);
        //tEnv.executeSql("CREATE DATABASE IF NOT EXISTS myhive.mydb WITH('k1' = 'a', 'k2' = 'b')");
        //tEnv.executeSql("CREATE TABLE IF NOT EXISTS myhive.mydb.mytable (name STRING, age INT) WITH ('connector' = 'print')");
        tEnv.useCatalog("myhive");
        tEnv.useDatabase("mydb");
        tEnv.from("myhive.mydb.mytable").printSchema();
    }
    }
