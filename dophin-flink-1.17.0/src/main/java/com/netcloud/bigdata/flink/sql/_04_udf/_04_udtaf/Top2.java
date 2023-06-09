package com.netcloud.bigdata.flink.sql._04_udf._04_udtaf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @author netcloud
 * @date 2023-06-09 15:51:35
 * @email netcloudtec@163.com
 * @description
 * 自定义UDATF函数（多进多出）,求每个WaterSensor中最高的两个水位值
 */
public  class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, vCTop2> {
    //创建累加器
    @Override
    public vCTop2 createAccumulator() {
        return new vCTop2();
    }

    //比较数据，如果当前数据大于累加器中存的数据则替换，并将原累加器中的数据往下（第二）赋值
    public void accumulate(vCTop2 acc, Integer value) {
        if (value > acc.first) {
            acc.second = acc.first;
            acc.first = value;
        } else if (value > acc.second) {
            acc.second = value;
        }
    }
    //计算（排名）
    public void emitValue(vCTop2 acc, Collector<Tuple2<Integer, Integer>> out) {
    // emit the value and rank
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}
