package com.netcloud.bigdata.flink.sql._03_sqlgrammar._03_other;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * @author netcloud
 * @date 2023-06-08 11:47:37
 * @email netcloudtec@163.com
 * @description
 * 1) SHOW 语句用于列出其相应父对象中的对象，例如 catalog、database、table 和 view、column、function 和 module。有关详细信息和其他选项，请参见各个命令。
 * 2) SHOW CREATE 语句用于打印给定对象的创建 DDL 语句。当前的 SHOW CREATE 语句仅在打印给定表和视图的 DDL 语句时可用。
 * 3) 可以使用 TableEnvironment 中的 executeSql() 方法执行 SHOW 语句。 若 SHOW 操作执行成功，executeSql() 方法返回所有对象，否则会抛出异常。
 * SHOW CATALOGS
 * SHOW CURRENT CATALOG
 * SHOW DATABASES
 * SHOW CURRENT DATABASE
 * SHOW TABLES
 * SHOW CREATE TABLE
 * SHOW COLUMNS
 * SHOW VIEWS
 * SHOW CREATE VIEW
 * SHOW FUNCTIONS
 * SHOW MODULES
 * SHOW JARS
 * SHOW JOBS
 */
public class ShowExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //TODO 将hive catalog配置
        String name = "myhive";
        String defaultDatabase = "mydb";
        String hiveConfDir = "/Users/yangshaojun/Bigdata_AI/data_center_workspace/dophin/data/hive-conf";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);
        // show modules
        tEnv.executeSql("SHOW MODULES").print();
        // show full modules
        tEnv.executeSql("SHOW FULL MODULES").print();

        // show catalogs
        tEnv.executeSql("SHOW CATALOGS").print();
        // show current catalogs
        tEnv.useCatalog("myhive");
        tEnv.executeSql("SHOW CURRENT CATALOG").print();

        // show databses
        tEnv.executeSql("SHOW DATABASES").print();
        tEnv.useDatabase("mydb");
        // show current databses
        tEnv.executeSql("SHOW CURRENT DATABASE").print();

        // show tables
        String source_table = "CREATE TABLE IF NOT EXISTS source_table (\n" +
                "            user_id BIGINT COMMENT '用户 id',\n" +
                "            name STRING COMMENT '用户姓名',\n" +
                "            server_timestamp BIGINT COMMENT '用户访问时间戳',\n" +
                "            proctime AS PROCTIME()\n" +
                "        ) WITH (\n" +
                "          'connector' = 'datagen',\n" +
                "          'rows-per-second' = '1',\n" +
                "          'fields.name.length' = '1',\n" +
                "          'fields.user_id.min' = '1',\n" +
                "          'fields.user_id.max' = '10',\n" +
                "          'fields.server_timestamp.min' = '1',\n" +
                "          'fields.server_timestamp.max' = '100000'\n" +
                "        )";
        tEnv.executeSql(source_table);
        tEnv.executeSql("SHOW TABLES").print();

        // show views
        tEnv.executeSql("CREATE VIEW IF NOT EXISTS my_view AS SELECT * FROM source_table");
        tEnv.executeSql("SHOW VIEWS").print();

        tEnv.loadModule("hive",new HiveModule("2.3.3"));

        // show functions
        tEnv.executeSql("SHOW FUNCTIONS").print();

        // create a user defined function
        // tEnv.executeSql("CREATE FUNCTION f1 AS ...").print();
        // tEnv.executeSql("CREATE USER FUNCTIONS").print();

    }
}
