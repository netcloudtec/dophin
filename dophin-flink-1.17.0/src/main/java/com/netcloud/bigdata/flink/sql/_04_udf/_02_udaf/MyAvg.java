package com.netcloud.bigdata.flink.sql._04_udf._02_udaf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author netcloud
 * @date 2023-06-09 15:36:01
 * @email netcloudtec@163.com
 * @description
 * 自定义UDAF函数,求每个WaterSensor中VC的平均值
 */
public  class MyAvg extends AggregateFunction<Double, MyAvgAccumulator> {
    //创建一个累加器
    @Override
    public MyAvgAccumulator createAccumulator() {
        return new MyAvgAccumulator();
    }

    //做累加操作
    public void accumulate(MyAvgAccumulator acc, Integer vc) {
        acc.sum += vc;
        acc.count += 1;
    }
    //将计算结果值返回
    @Override
    public Double getValue(MyAvgAccumulator accumulator) {
        return accumulator.sum*1D/accumulator.count;
    }
}
