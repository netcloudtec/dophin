package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._04_lookup_join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-31 11:23:37
 * @email netcloudtec@163.com
 * @description
 * 维表join
 */
public class MySQLLookupExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String exampleSql = "CREATE TABLE show_log (\n"
                + "    log_id BIGINT,\n"
                + "    `timestamp` as cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    user_id STRING,\n"
                + "    proctime AS PROCTIME()\n"
                + ")\n"
                + "WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.user_id.length' = '1',\n"
                + "  'fields.log_id.min' = '1',\n"
                + "  'fields.log_id.max' = '10'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE user_profile (\n" +
                "  user_id STRING,\n" +
                "  age STRING,\n" +
                "  sex STRING,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test_db',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'Sunmnet@123',\n" +
                "   'table-name' = 'users'\n" +
                ");"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    log_id BIGINT,\n"
                + "    `timestamp` TIMESTAMP(3),\n"
                + "    user_id STRING,\n"
                + "    proctime TIMESTAMP(3),\n"
                + "    age STRING,\n"
                + "    sex STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT \n"
                + "    s.log_id as log_id\n"
                + "    , s.`timestamp` as `timestamp`\n"
                + "    , s.user_id as user_id\n"
                + "    , s.proctime as proctime\n"
                + "    , u.sex as sex\n"
                + "    , u.age as age\n"
                + "FROM show_log AS s\n"
                + "LEFT JOIN user_profile FOR SYSTEM_TIME AS OF s.proctime AS u\n"
                + "ON s.user_id = u.user_id";

        Arrays.stream(exampleSql.split(";"))
                .forEach(tableEnv::executeSql);

    }
}
