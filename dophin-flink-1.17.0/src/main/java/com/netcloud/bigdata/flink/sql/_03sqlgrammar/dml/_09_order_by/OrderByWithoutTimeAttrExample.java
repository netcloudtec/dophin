package com.netcloud.bigdata.flink.sql._03sqlgrammar.dml._09_order_by;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Arrays;

/**
 * @author netcloud
 * @date 2023-05-31 17:04:23
 * @email netcloudtec@163.com
 * @description
 */
public class OrderByWithoutTimeAttrExample {
    public static void main(String[] args) {
        /**
         * Exception in thread "main" org.apache.flink.table.api.TableException: Sort on a non-time-attribute field
         * is not supported.
         * 	at org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort.translateToPlanInternal
         * 	(StreamExecSort.java:75)
         * 	at org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:134)
         * 	at org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:247)
         * 	at org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink.translateToPlanInternal
         * 	(StreamExecSink.java:104)
         * 	at org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:134)
         * 	at org.apache.flink.table.planner.delegation.StreamPlanner$$anonfun$translateToPlan$1.apply(StreamPlanner
         * 	.scala:70)
         * 	at org.apache.flink.table.planner.delegation.StreamPlanner$$anonfun$translateToPlan$1.apply(StreamPlanner
         * 	.scala:69)
         * 	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
         * 	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
         * 	at scala.collection.Iterator$class.foreach(Iterator.scala:891)
         * 	at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
         * 	at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
         * 	at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
         * 	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
         * 	at scala.collection.AbstractTraversable.map(Traversable.scala:104)
         * 	at org.apache.flink.table.planner.delegation.StreamPlanner.translateToPlan(StreamPlanner.scala:69)
         * 	at org.apache.flink.table.planner.delegation.PlannerBase.translate(PlannerBase.scala:165)
         * 	at org.apache.flink.table.api.internal.TableEnvironmentImpl.translate(TableEnvironmentImpl.java:1518)
         * 	at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeInternal(TableEnvironmentImpl.java:740)
         * 	at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeInternal(TableEnvironmentImpl.java:856)
         * 	at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:730)
         * 	at java.util.Spliterators$ArraySpliterator.forEachRemaining(Spliterators.java:948)
         * 	at java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:580)
         */
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
                //.inBatchMode()//声明批流任务
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String sql = "CREATE TABLE source_table_1 (\n"
                + "    user_id BIGINT NOT NULL\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.user_id.min' = '1',\n"
                + "  'fields.user_id.max' = '10'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE sink_table (\n"
                + "    user_id BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ");\n"
                + "\n"
                + "INSERT INTO sink_table\n"
                + "SELECT user_id\n"
                + "FROM source_table_1\n"
                + "Order By user_id\n";

        Arrays.stream(sql.split(";"))
                .forEach(tableEnv::executeSql);
    }
}
