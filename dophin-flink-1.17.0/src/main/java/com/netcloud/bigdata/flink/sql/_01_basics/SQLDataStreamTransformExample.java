package com.netcloud.bigdata.flink.sql._01_basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author netcloud
 * @date 2023-06-20 10:31:42
 * @email netcloudtec@163.com
 * @description SQL与DataStream转换
 */
public class SQLDataStreamTransformExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1. pdd 发补贴券流水数据
        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n"
                + "    money BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";
        // 2. 计算总计发放补贴券的金额
        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n"
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        tableEnv.executeSql(createTableSql);
        Table resultTable = tableEnv.sqlQuery(querySql);

        // 3. 将金额结果转为 DataStream，然后自定义超过 1w 的报警逻辑
        tableEnv
                .toDataStream (resultTable, Row.class)
                .flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public void flatMap(Row value, Collector<Object> out) throws Exception {
                        long l = Long.parseLong(String.valueOf(value.getField("sum_money")));
                        if (l > 10000L) {
                            System.out.println("报警，超过 1w");
                        }
                    }
                });
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
