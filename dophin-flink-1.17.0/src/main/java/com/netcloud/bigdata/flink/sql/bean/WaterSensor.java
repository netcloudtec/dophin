package com.netcloud.bigdata.flink.sql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author netcloud
 * @date 2023-06-09 15:38:59
 * @email netcloudtec@163.com
 * @description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    //id:传感器编号
    private String id;
    //时间戳
    private Long ts;
    //水位
    private Integer vc;
}
