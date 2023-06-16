package com.netcloud.bigdata.flink.sql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author netcloud
 * @date 2023-06-16 11:35:22
 * @email netcloudtec@163.com
 * @description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Click {
    private String userName;
    private String cTime;
    private String url;
}
