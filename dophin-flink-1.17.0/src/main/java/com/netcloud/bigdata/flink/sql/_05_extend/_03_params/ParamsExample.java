package com.netcloud.bigdata.flink.sql._05_extend._03_params;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-09 18:18:26
 * @email netcloudtec@163.com
 * @description
 * 任务参数配置
 * 参数设置方式
 * 1、FlinkSQL相关参数需要在TableEnvironmnet中设置
 * Configuration configuration = tEnv.getConfig().getConfiguration();
 * configuration.setString("key","value");
 * 2、具体参数分为如下三类
 * 1)运行时参数:优化Flink SQL任务在执行时任务性能
 *   最常用的两种就是Minibatch聚合
 *   state ttl状态过期
 * 2)优化器参数:FlinkSQL任务在生成执行计划时，经过优化器优化生成更优的执行计划
 * 3)表参数:用来调整Flink SQL table的执行行为。
 *
 */
public class ParamsExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration conf = tableEnv.getConfig().getConfiguration();
        conf.setString("", "");
        /**
         * 运行时参数:
         * # 默认值：100
         * # 值类型：Integer
         * # 流批任务：流、批任务都支持
         * # 用处：异步 lookup join 中最大的异步 IO 执行数目
         * # 补充说明：该参数很少使用。如果是维度join，一般会在 Flink内部执行
         * 1) table.exec.async-lookup.buffer-capacity:100
         *
         * # 默认值：false
         * # 值类型：Boolean
         * # 流批任务：流任务支持
         * # 用处：MiniBatch 优化是一种专门针对 unbounded 流任务的优化（即非窗口类应用），其机制是在 `允许的延迟时间间隔内` 以及 `达到最大缓冲记录数`
         *   时触发以减少 `状态访问` 的优化，从而节约处理时间。下面两个参数一个代表 `允许的延迟时间间隔`，另一个代表 `达到最大缓冲记录数`。
         * 2) table.exec.mini-batch.enabled: false
         *
         * # 默认值：0 ms
         * # 值类型：Duration
         * # 流批任务：流任务支持
         * # 用处：此参数设置为多少就代表 MiniBatch 机制最大允许的延迟时间。注意这个参数要配合 `table.exec.mini-batch.enabled` 为 true 时使用，而且必须大于 0 ms
         * table.exec.mini-batch.allow-latency: 0 ms
         *
         * # 默认值：-1
         * # 值类型：Long
         * # 流批任务：流任务支持
         * # 用处：此参数设置为多少就代表 MiniBatch 机制最大缓冲记录数。注意这个参数要配合 `table.exec.mini-batch.enabled` 为 true 时使用，而且必须大于 0
         * table.exec.mini-batch.size: -1
         *
         * # 默认值：-1
         * # 值类型：Integer
         * # 流批任务：流、批任务都支持
         * # 用处：可以用此参数设置 Flink SQL 中算子的并行度，这个参数的优先级 `高于` StreamExecutionEnvironment 中设置的并行度优先级，如果这个值设置为 -1，则代表没有设置，会默认使用 StreamExecutionEnvironment 设置的并行度
         * 3) table.exec.resource.default-parallelism: -1
         *
         * # 默认值：ERROR
         * # 值类型：Enum【ERROR, DROP】
         * # 流批任务：流、批任务都支持
         * # 用处：表上的 NOT NULL 列约束强制不能将 NULL 值插入表中。Flink 支持 `ERROR`（默认）和 `DROP` 配置。默认情况下，当 NULL 值写入 NOT NULL 列时，Flink 会产生运行时异常。用户可以将行为更改为 `DROP`，直接删除此类记录，而不会引发异常。
         * 4) table.exec.sink.not-null-enforcer: ERROR
         *
         * # 默认值：false
         * # 值类型：Boolean
         * # 流批任务：流任务
         * # 用处：接入了 CDC 的数据源，上游 CDC 如果产生重复的数据，可以使用此参数在 Flink 数据源算子进行去重操作，去重会引入状态开销
         * 5) table.exec.source.cdc-events-duplicate: false
         *
         * # 默认值：0 ms
         * # 值类型：Duration
         * # 流批任务：流任务
         * # 用处：如果此参数设置为 60 s，当 Source 算子在 60 s 内未收到任何元素时，这个 Source 将被标记为临时空闲，此时下游任务就不依赖此 Source 的 Watermark 来推进整体的 Watermark 了。
         * # 默认值为 0 时，代表未启用检测源空闲。
         * 6) table.exec.source.idle-timeout: 0 ms
         *
         * # 默认值：0 ms
         * # 值类型：Duration
         * # 流批任务：流任务
         * # 用处：指定空闲状态（即未更新的状态）将保留多长时间。尤其是在 unbounded 场景中很有用。默认 0 ms 为不清除空闲状态
         * 7) table.exec.state.ttl: 0 ms
         *
         */

        /**
         * 优化器参数
         * #  默认值：AUTO
         * #  值类型：String
         * #  流批任务：流、批任务都支持
         * #  用处：聚合阶段的策略。和 MapReduce 的 Combiner 功能类似，可以在数据 shuffle 前做一些提前的聚合，可以选择以下三种方式
         * #  TWO_PHASE：强制使用具有 localAggregate 和 globalAggregate 的两阶段聚合。请注意，如果聚合函数不支持优化为两个阶段，Flink 仍将使用单阶段聚合。
         * #  两阶段优化在计算 count，sum 时很有用，但是在计算 count distinct 时需要注意，key 的稀疏程度，如果 key 不稀疏，那么很可能两阶段优化的效果会适得其反
         * #  ONE_PHASE：强制使用只有 CompleteGlobalAggregate 的一个阶段聚合。
         * #  AUTO：聚合阶段没有特殊的执行器。选择 TWO_PHASE 或者 ONE_PHASE 取决于优化器的成本。
         * #  注意！！！：此优化在窗口聚合中会自动生效，但是在 unbounded agg 中需要与 minibatch 参数相结合使用才会生效
         * 1) table.optimizer.agg-phase-strategy: AUTO
         *
         * #  默认值：false
         * #  值类型：Boolean
         * #  流批任务：流任务
         * #  用处：避免 group by 计算 count distinct\sum distinct 数据时的 group by 的 key 较少导致的数据倾斜，比如 group by 中一个 key 的 distinct 要去重 500w 数据，而另一个 key 只需要去重 3 个 key，那么就需要先需要按照 distinct 的 key 进行分桶。将此参数设置为 true 之后，下面的 table.optimizer.distinct-agg.split.bucket-num 可以用于决定分桶数是多少
         * #  后文会介绍具体的案例
         * 2) table.optimizer.distinct-agg.split.enabled: false
         *
         * #  默认值：1024
         * #  值类型：Integer
         * #  流批任务：流任务
         * #  用处：避免 group by 计算 count distinct 数据时的 group by 较少导致的数据倾斜。加了此参数之后，会先根据 group by key 结合 hash_code（distinct_key）进行分桶，然后再自动进行合桶。
         * #  后文会介绍具体的案例
         * 3) table.optimizer.distinct-agg.split.bucket-num: 1024
         *
         * #  默认值：true
         * #  值类型：Boolean
         * #  流批任务：流任务
         * #  用处：如果设置为 true，Flink 优化器将会尝试找出重复的自计划并重用。默认为 true 不需要改动
         * 4) table.optimizer.reuse-sub-plan-enabled: true
         *
         * #  默认值：true
         * #  值类型：Boolean
         * #  流批任务：流任务
         * #  用处：如果设置为 true，Flink 优化器会找出重复使用的 table source 并且重用。默认为 true 不需要改动
         * 5) table.optimizer.reuse-source-enabled: true
         *
         * #  默认值：true
         * #  值类型：Boolean
         * #  流批任务：流任务
         * #  用处：如果设置为 true，Flink 优化器将会做谓词下推到 FilterableTableSource 中，将一些过滤条件前置，提升性能。默认为 true 不需要改动
         * 6) table.optimizer.source.predicate-pushdown-enabled: true
         */

        /**
         * #  默认值：false
         * #  值类型：Boolean
         * #  流批任务：流、批任务都支持
         * #  用处：DML SQL（即执行 insert into 操作）是异步执行还是同步执行。默认为异步（false），即可以同时提交多个 DML SQL 作业，如果设置为 true，则为同步，第二个 DML 将会等待第一个 DML 操作执行结束之后再执行
         * 1) table.dml-sync: false
         *
         * #  默认值：64000
         * #  值类型：Integer
         * #  流批任务：流、批任务都支持
         * #  用处：Flink SQL 会通过生产 java 代码来执行具体的 SQL 逻辑，但是 jvm 限制了一个 java 方法的最大长度不能超过 64KB，但是某些场景下 Flink SQL 生产的 java 代码会超过 64KB，这时 jvm 就会直接报错。因此此参数可以用于限制生产的 java 代码的长度来避免超过 64KB，从而避免 jvm 报错。
         * 2) table.generated-code.max-length: 64000
         *
         * #  默认值：default
         * #  值类型：String
         * #  流批任务：流、批任务都支持
         * #  用处：在使用天级别的窗口时，通常会遇到时区问题。举个例子，Flink 开一天的窗口，默认是按照 UTC 零时区进行划分，那么在北京时区划分出来的一天的窗口是第一天的早上 8:00 到第二天的早上 8:00，但是实际场景中想要的效果是第一天的早上 0:00 到第二天的早上 0:00 点。因此可以将此参数设置为 GMT+08:00 来解决这个问题。
         * # 推荐使用timestamp_ltz(带时区的)，timestamp(不带时区)
         * 3) table.local-time-zone: default
         *
         * #  默认值：default
         * #  值类型：Enum【BLINK、OLD】
         * #  流批任务：流、批任务都支持
         * #  用处：Flink SQL planner，默认为 BLINK planner，也可以选择 old planner，但是推荐使用 BLINK planner
         * 4) table.planner: BLINK
         *
         * #  默认值：default
         * #  值类型：String
         * #  流批任务：流、批任务都支持
         * #  用处：Flink 解析一个 SQL 的解析器，目前有 Flink SQL 默认的解析器和 Hive SQL 解析器，其区别在于两种解析器支持的语法会有不同，比如 Hive SQL 解析器支持 between and、rlike 语法，Flink SQL 不支持
         * 5) table.sql-dialect: default
         */
    }
}
