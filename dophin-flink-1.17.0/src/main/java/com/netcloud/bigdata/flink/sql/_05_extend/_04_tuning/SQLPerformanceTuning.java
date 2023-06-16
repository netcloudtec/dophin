package com.netcloud.bigdata.flink.sql._05_extend._04_tuning;

import com.netcloud.bigdata.flink.sql.bean.Click;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author netcloud
 * @date 2023-06-16 11:30:38
 * @email netcloudtec@163.com
 * @description
 * /**
/**
 * SQL聚合算子性能优化
 * 四种优化:
 * 1) (常用)MiniBatch聚合 (满足两者中的其中一个条件就会执行计算，减少访问状态时间开销)
 * 2) (常用)两阶段聚合 (解决热点数据导致的数据倾斜，核心思想类似于MR的Combiner+Reduce)
 * 3) (不常用)Split分桶 (count(distinct)、sum(distinct)解决两阶段聚合不足问题,核心思想就是将distinct key中的key进行数据分桶打散，分散到Flink的多个TM上执行，然后再将数据合桶计算)
 * 4) (常用)去重Filter字句
 *
 * --hostname 127.0.0.1 --port 9999
 * nc -lk 9999
 * Mary,12:00:00,./home
 * Mary,12:00:00,./home
 * Mary,12:00:00,./home
 * Mary,12:00:00,./home
 *
 */
public class SQLPerformanceTuning {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration conf = tableEnv.getConfig().getConfiguration();
        //1、(常用)MiniBatch聚合优化，适用于非窗口无界流聚合，达到微批计算，减少每条计算时候访问state的时长。
        conf.setString("table.exec.mini-batch.enabled", "true");
        conf.setString("table.exec.mini-batch.allow-latency", "5 s");//5s执行计算一次
        conf.setString("table.exec.mini-batch.size", "3");//数据达到3条时计算一次，与上面的时间延迟是或的关系
        //2、(常用)两阶段聚合。
        conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE"); //两阶段聚合，
        //3、(不常用)split分桶。
        conf.setString("table.optimizer.distinct-agg.split.enabled", "true");//开启split分桶支持

        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 9999);
        DataStream<String> inputDataStream = env.socketTextStream(hostname, port);

        //TODO 使用点击流数据测试MiniBatch聚合
        DataStream<Click> mapDS = inputDataStream.map(new MapFunction<String, Click>() {
            @Override
            public Click map(String content) throws Exception {
                String[] strArr = content.split(",");
                return new Click(strArr[0], strArr[1], strArr[2]);
            }
        });

        tableEnv.createTemporaryView("clicks",mapDS);
        Table table = tableEnv.sqlQuery("select userName,count(url) as cnt from clicks group by userName");
        tableEnv.toChangelogStream(table).print();

        //TODO 去重Fliter字句
        /**
         * 1、使用Filter字句前
         * SELECT
         *  day,
         *  COUNT(DISTINCT user_id) AS total_uv,
         *  COUNT(DISTINCT CASE WHEN flag IN ('android', 'iphone') THEN user_id ELSE NULL END) AS app_uv,
         *  COUNT(DISTINCT CASE WHEN flag IN ('wap', 'other') THEN user_id ELSE NULL END) AS web_uv
         * FROM T
         * GROUP BY day
         * 2、使用Filter字句后
         * SELECT
         *  day,
         *  COUNT(DISTINCT user_id) AS total_uv,
         *  COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('android', 'iphone')) AS app_uv,
         *  COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('wap', 'other')) AS web_uv
         * FROM T
         * GROUP BY day
         */

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
