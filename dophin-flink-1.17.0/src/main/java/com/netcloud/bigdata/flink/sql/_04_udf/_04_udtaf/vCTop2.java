package com.netcloud.bigdata.flink.sql._04_udf._04_udtaf;

/**
 * @author netcloud
 * @date 2023-06-09 15:51:23
 * @email netcloudtec@163.com
 * @description
 */
//定义一个类当做累加器，并声明第一和第二这两个值
public class vCTop2 {
    public Integer first = Integer.MIN_VALUE;
    public Integer second = Integer.MIN_VALUE;
}
