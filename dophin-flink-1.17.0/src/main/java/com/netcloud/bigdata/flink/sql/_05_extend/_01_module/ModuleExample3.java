package com.netcloud.bigdata.flink.sql._05_extend._01_module;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Arrays;


/**
 * @author netcloud
 * @date 2023-06-09 17:02:11
 * @email netcloudtec@163.com
 * @description
 * FlinkSQL 支持Hive UDF
 * 1)Hive扩展支持hive内置UDF
 * 2)Hive扩展支持用户自定义的Hive UDF
 */
public class ModuleExample3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1、FlinkSQL扩展支持内置的udf，maven中引入hive的connector
        tableEnv.loadModule("hive",new HiveModule("2.3.3"));
        String[] modules = tableEnv.listModules();
        Arrays.stream(modules).forEach(System.out::println);

        //查看所有module下的所有udf
        String[] functions = tableEnv.listFunctions();
        //发现hive module下的get_json_object
        Arrays.stream(functions).forEach(System.out::println);

        //使用get_json_object函数
        DataStreamSource<String> waterSensorDataStreamSource = env.fromElements("{\"ip\":\"102\",\"moniterType\":\"healthCheck\",\"carNum\":\"DFHYD04777\",\"agentName\":\"systemAgent\"}");
        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);
        // 注册临时视图
        tableEnv.createTemporaryView("source_table", table);
        tableEnv.sqlQuery("SELECT get_json_object(f0,'$.ip')  FROM source_table").execute().print();
        //2、FlinkSQL扩展支持用户自定义的hive udf的增强module；详细见文档

    }
}
