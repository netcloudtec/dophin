package com.netcloud.bigdata.flink.sql._03_sqlgrammar._03_other;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author netcloud
 * @date 2023-06-08 11:43:36
 * @email netcloudtec@163.com
 * @description
 * FlinkSQL中使用Use字句进行切换CataLog和DataBase或者使用Module
 * 可以使用 TableEnvironment 中的 executeSql() 方法执行 USE 语句。 若 USE 操作执行成功，executeSql() 方法返回 ‘OK’，否则会抛出异常。
 * 详细使用过程参考 com.netcloud.bigdata.flink.sql._05_extend._03_catalogs.HiveCataLog
 */
public class UseExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 设置hdfs 用户名称
        System.setProperty("HADOOP_USER_NAME", "hadoop");

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

        //使用hive已经存在的库
        tEnv.useCatalog("myhive");
        tEnv.useDatabase("zhidao");
        tEnv.sqlQuery("SELECT * FROM myhive.zhidao.dwd_zhidao_autopilot_oper_order_info limit 10").execute().print();
    }
}
