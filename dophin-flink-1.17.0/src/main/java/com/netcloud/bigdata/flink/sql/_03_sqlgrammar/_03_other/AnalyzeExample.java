package com.netcloud.bigdata.flink.sql._03_sqlgrammar._03_other;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * @author netcloud
 * @date 2023-06-08 14:58:46
 * @email netcloudtec@163.com
 * @description
 * 1) ANALYZE 语句被用于为存在的表收集统计信息，并将统计信息写入该表的 catalog 中。当前版本中，ANALYZE 语句只支持 ANALYZE TABLE， 且只能由用户手动触发。
 * 注意 现在, ANALYZE TABLE 只支持批模式（Batch Mode），且只能用于已存在的表， 如果表不存在或者是视图（View）则会报错。
 * 2) FlinkSQL读取Hive数据
 * a)本地安装hadoop版本和生产环境hadoop版本一致
 * b)System.setProperty("HADOOP_USER_NAME", "hadoop");指定权限用户
 * c)指定hive存在的库
 */
public class AnalyzeExample {
    public static void main(String[] args) {
        final EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
//                .inStreamingMode() //声明为流任务
                .inBatchMode()     //声明批流任务
                .build();
        final TableEnvironment tEnv = TableEnvironment.create(settings);

        // 设置hdfs 用户名称
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        String name = "myhive";
        // hive 中的库名
        String defaultDatabase = "zhidao";
        String hiveConfDir = "/Users/yangshaojun/Bigdata_AI/data_center_workspace/dophin/data/hive-conf";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);
        tEnv.useCatalog("myhive");
        // hive 中的库名
        tEnv.useDatabase("zhidao");

        //FlinkSQL 查询Hive表中的数据
        tEnv.sqlQuery("SELECT * FROM dwd_zhidao_autopilot_oper_order_info limit 10").execute().print();
        //非分区表，收集表级别的统计信息(表的统计信息主要为行数(row count))。
        tEnv.executeSql("ANALYZE TABLE dwd_zhidao_autopilot_oper_order_info COMPUTE STATISTICS");
    }
}
