package com.netcloud.bigdata.flink.sql._04_udf._02_udaf;

/**
 * @author netcloud
 * @date 2023-06-09 15:35:29
 * @email netcloudtec@163.com
 * @description
 * 定义一个类当做累加器，并声明总数和总个数这两个值
 */
public class MyAvgAccumulator {
    public long sum = 0;

    public int count = 0;
}
