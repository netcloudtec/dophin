package com.netcloud.bigdata.flink.sql._05_extend._03_catalogs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author netcloud
 * @date 2023-06-02 15:38:32
 * @email netcloudtec@163.com
 * @description
 * 创建Flink表,将其注册到CataLog
 * Catalog 提供了元数据信息，例如数据库、表、分区、视图以及数据库或其他外部系统中存储的函数和信息。
 * 1) 数据处理最关键的方面之一是管理元数据。 元数据可以是临时的，例如临时表、或者通过 TableEnvironment 注册的 UDF。
 * 2) 元数据也可以是持久化的，例如 Hive Metastore 中的元数据。
 * 3) Catalog 提供了一个统一的API，用于管理元数据，并使其可以从 Table API 和 SQL 查询语句中来访问。
 * Catalog分类
 * 1) GenericInMemoryCatalog (GenericInMemoryCatalog 是基于内存实现的 Catalog，所有元数据只在 session 的生命周期内可用。)
 * 2) JdbcCatalog (使得用户可以将 Flink 通过 JDBC 协议连接到关系数据库。Postgres Catalog 和 MySQL Catalog 是目前 JDBC Catalog 仅有的两种实现。)
 * 3) HiveCatalog(两个用途:Flink元数据的持久存储，读写现有Hive元数据接口)
 * 4) 自定义Catalog
 * 警告:
 * Hive Metastore 以小写形式存储所有元数据对象名称。而 GenericInMemoryCatalog 区分大小写。
 */
public class HiveCataLog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";
        String defaultDatabase = "mydb";
        String hiveConfDir     = "/Users/yangshaojun/Bigdata_AI/data_center_workspace/dophin/data/hive-conf";
        //如果默认库不存在会报错 Configured default database mydatabase doesn't exist in catalog myhive.
        //HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        HiveCatalog hive = new HiveCatalog(name, null, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);
        //创建当前catalog下的库
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS myhive.mydb WITH('k1' = 'a', 'k2' = 'b')");
        //创建当前catalog、database下的表
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS myhive.mydb.mytable (name STRING, age INT) WITH ('connector' = 'print')");
        tEnv.listCatalogs();
        //最后创建的表其元数据位于user/hive/warehouse/mydb.db目录下
    }
}
