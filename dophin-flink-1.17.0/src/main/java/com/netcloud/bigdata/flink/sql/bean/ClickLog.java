package com.netcloud.bigdata.flink.sql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author netcloud
 * @date 2023-05-25 10:25:24
 * @email netcloudtec@163.com
 * @description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickLog {
    private String logId;
    private String clickParams;
    private String eventTime;
}
