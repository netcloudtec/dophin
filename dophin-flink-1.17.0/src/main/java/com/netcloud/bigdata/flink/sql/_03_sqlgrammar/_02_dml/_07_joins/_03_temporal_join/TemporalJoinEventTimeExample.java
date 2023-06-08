package com.netcloud.bigdata.flink.sql._03_sqlgrammar._02_dml._07_joins._03_temporal_join;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * @author netcloud
 * @date 2023-05-26 10:24:59
 * @email netcloudtec@163.com
 * @description 时态连接
 * Note:
 * Flink1.17版本 输出KafkaSink立即挂掉，Flink1.15版本没有此问题
 * 暂时没有找到问题出现在了哪里、如果测试Temporal Join可暂时修改pom中的 flink-version
 *
 * Flink1.17版本CDC
 * 1、 Mysql5.7版本开启bin-log
 * 1) 修改配置文件 /etc/my.cnf
 * log-bin=mysql-bin
 * server-id=1
 * binlog_format=ROW
 * 2) 重启服务
 * service mysqld restart
 * 3) 添加flink-connector-mysql-cdc maven依赖自编译
 * 4) 添加mySQL mysql-connector-java manve依赖8.0.27v
 * 5) FlinkCDC 2.x版本无法读取增量数据，解决方案:程序中开启checkpoint
 *    a) env.enableCheckpointing() 直接配置
 *    b) 在TableEnvironment 中通过conf配置
 *
 * CREATE TABLE `book_price` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
 *   `book_name` varchar(20) DEFAULT NULL,
 *   `book_price` bigint(10) DEFAULT NULL,
 *   `stat_date` varchar(20) DEFAULT NULL,
 *   PRIMARY KEY (`id`) USING BTREE
 * ) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4;
 * INSERT INTO `book_price` VALUES (1, 'xxx', 12, '2021-07-24 00:00:00');
 *
 * //创建名为 flinksql_book 的 Topic
 * bin/kafka-topics.sh -zookeeper localhost:2181 --create --partitions 5 --replication-factor 1 --topic flinksql_book02
 *
 * //列出所有 Topic
 * bin/kafka-topics.sh --list --zookeeper localhost:2181
 *
 * //查询 Topic 的详细信息
 * bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic flinksql_book
 *
 * //生产者发送消息
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic flinksql_book02
 * o_001,11.11,EUR,2021-07-24 00:00:08
 * //消费者查询消息（从头开始）
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --from-beginning --topic flinksql_book
 * //从尾部开始取数据（从尾开始）
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_kafka_topic --offset latest
 *
 * //删除名为 flinksql_book 的 Topic
 * bin/kafka-topics.sh --delete --zookeeper localhost:2181  --topic flinksql_book01
 *
 * Note: 最后没有输出打印，不知道问题出现在哪里了 具体的测试看TemporalJoinEventTimeExample2示例
 */
public class TemporalJoinEventTimeExample {
    public static void main(String[] args) {
        //TODO 方法一: 通过EnvironmentSettings创建表环境，并设置checkpoint
        /**
         * EnvironmentSettings settings = EnvironmentSettings
         *         .newInstance()
         *         .inStreamingMode() //声明为流任务
         *         //.inBatchMode()//声明批流任务
         *         .build();
         * TableEnvironment tableEnv = TableEnvironment.create(settings);
         * Configuration configuration = tableEnv.getConfig().getConfiguration();
         * configuration.setString("execution.checkpointing.interval","5000");
         * configuration.setString("execution.checkpointing.mode","EXACTLY_ONCE");
         * configuration.setString("state.backend","filesystem");
         * configuration.setString("state.checkpoints.dir","file:////Users/yangshaojun/Bigdata_AI/data_center_workspace/dophin/data/flink_checkpoint");
         */
        //TODO 方法二: 通过StreamExecutionEnvironment创建表环境，并设置checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:////Users/yangshaojun/Bigdata_AI/data_center_workspace/dophin/data/flink_checkpoint");
        /**
         * o_001,11.11,EUR,2021-07-24 00:00:08
         * o_001,11.11,EUR,2021-07-24 00:00:10
         * o_002,11.11,Yen,2021-07-25 02:00:10
         */
        String orderSource = "CREATE TABLE orders (\n" +
                " currency STRING,\n" +
                " event_time STRING,\n" +
                "rowtime AS CAST(TO_TIMESTAMP(`event_time`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3)),\n" +
                " WATERMARK FOR rowtime AS rowtime\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'order',\n" +
                "  'properties.bootstrap.servers' = '10.2.9.120:9092',\n" +
                "  'properties.group.id' = 'order',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ");";

        /** 使用kafka作为快照表无法关联不知道问题出现在哪
         String rateSource = "CREATE TABLE currency_rates (\n" +
         " currency STRING,\n" +
         " conversion_rate DECIMAL(32, 2),\n" +
         " event_time TIMESTAMP(3),\n" +
         " WATERMARK FOR event_time AS event_time,\n" +
         " PRIMARY KEY(currency) NOT ENFORCED\n" +
         ") WITH (\n" +
         "  'connector' = 'kafka',\n" +
         "  'topic' = 'rates_historys',\n" +
         "  'properties.bootstrap.servers' = '10.2.9.120:9092',\n" +
         "  'properties.group.id' = 'rates_history',\n" +
         "  'scan.startup.mode' = 'earliest-offset',\n" +
         "  'format' = 'debezium-json'\n" +
         ");";
         **/

        /** 使用MySQL作为快照表无法关联不知道问题出现在哪
         String mySQLSource="CREATE TABLE mysql_binlog (\n" +
         " id STRING,\n" +
         " book_name STRING,\n" +
         " book_price BIGINT,\n" +
         " stat_date STRING,\n" +
         " event_time AS CAST(TO_TIMESTAMP(`stat_date`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3)),\n" +
         " WATERMARK FOR event_time AS event_time,\n" +
         " PRIMARY KEY(id) NOT ENFORCED\n" +
         ") WITH (\n" +
         "  'connector' = 'mysql-cdc',\n" +
         "  'hostname' = '172.16.240.21',\n" +
         "  'port' = '3306',\n" +
         "  'username' = 'root',\n" +
         "  'password' = 'Sunmnet@123',\n" +
         "  'server-time-zone' = 'Asia/Shanghai',\n" +
         "  'debezium.snapshot.mode' = 'initial',\n" +
         "  'database-name' = 'cdc_demo',\n" +
         "  'table-name' = 'book_price'\n" +
         ")";
         **/


        List<Tuple3<String, Long, String>> rateHistoryData = Lists.newArrayList();
        rateHistoryData.add(new Tuple3<>("EUR", 114L, "2021-07-24 00:00:02"));

        DataStream<Tuple3<String, Long, String>> rateStream = env.fromCollection(rateHistoryData);

        Table rateTable = tableEnv.fromDataStream(
                rateStream,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "CAST(TO_TIMESTAMP(`f2`, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3))")
                        .watermark("rowtime","rowtime - interval '5' SECOND")
                        .primaryKey("f0")
                        .build());

        tableEnv.createTemporaryView("currency_rates", rateTable);

        String sqlQuery = "SELECT *" +
                "FROM " +
                " orders AS o " +
                " LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.rowtime AS r " +
                "ON o.currency = r.f0";

        tableEnv.executeSql(orderSource);
        //tableEnv.executeSql(rateSource);
        //rateTable.printSchema();//only for debug
        //tableEnv.from("currency_rates").execute().print();

//        tableEnv.sqlQuery("SELECT * FROM orders").execute().print();
//        tableEnv.sqlQuery("SELECT * FROM currency_rates").execute().print();

        tableEnv.sqlQuery(sqlQuery).execute().print();

    }
}
