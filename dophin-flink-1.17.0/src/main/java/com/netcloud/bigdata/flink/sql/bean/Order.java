package com.netcloud.bigdata.flink.sql.bean;

import lombok.Data;

/**
 * @author netcloud
 * @date 2023-05-19 14:34:49
 * @email netcloudtec@163.com
 * @description
 */
@Data
public class Order {
    public String orderNo;
    public String name;
    public Long timestamp;

    public Order() {
    }

    public Order(String orderNo, String name, Long timestamp) {
        this.orderNo = orderNo;
        this.name = name;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderNo='" + orderNo + '\'' +
                ", name=" + name +
                ", timestamp=" + timestamp +
                '}';
    }
}
