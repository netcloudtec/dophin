package com.netcloud.bigdata.flink.sql.bean;

import org.apache.flink.table.annotation.DataTypeHint;

import java.math.BigDecimal;

/**
 * 自定义数据类型
 */
public class User {
    // 1. 基础类型，Flink 可以通过反射类型信息自动把数据类型获取到
    public int age;
    public String name;

    // 2. 复杂类型，用户可以通过 @DataTypeHint("DECIMAL(10, 2)") 注解标注此字段的数据类型
    public @DataTypeHint("DECIMAL(10, 2)")
    BigDecimal totalBalance;
}
