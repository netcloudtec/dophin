package com.netcloud.bigdata.flink.sql.bean;

import lombok.Data;

/**
 * @author netcloud
 * @date 2023-05-19 14:20:30
 * @email netcloudtec@163.com
 * @description
 */
@Data
public  class Event {
    public String status;
    public Long id;
    public Long timestamp;
    public Event() {

    }
    public Event(String status, Long id, Long timestamp) {
        this.status = status;
        this.id = id;
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "Event{" +
                "status='" + status + '\'' +
                ", id='" + id + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
