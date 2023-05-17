package com.netcloud.bigdata.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author netcloud
 * @date 2023-05-16 15:39:07
 * @email netcloudtec@163.com
 * @description
 */
public class Mod_UDF extends ScalarFunction {

    public int eval(long id, int remainder) {
        return (int) (id % remainder);
    }

}
