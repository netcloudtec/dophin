package com.netcloud.bigdata.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author netcloud
 * @date 2023-05-19 14:41:13
 * @email netcloudtec@163.com
 * @description
 * 继承ScalarFunction类，重写eval方法
 * 自定义函数将小写字符串转为大写
 */
public class ToUpper_UDF extends ScalarFunction {

    public String eval(String status) {
        return status.toUpperCase();
    }
}
