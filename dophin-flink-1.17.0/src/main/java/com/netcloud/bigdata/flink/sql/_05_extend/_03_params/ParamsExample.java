package com.netcloud.bigdata.flink.sql._05_extend._03_params;

/**
 * @author netcloud
 * @date 2023-06-09 18:18:26
 * @email netcloudtec@163.com
 * @description
 * 任务参数配置
 * 参数设置方式
 * 1、FlinkSQL相关参数需要在TableEnvironmnet中设置
 * Configuration configuration = tEnv.getConfig().getConfiguration();
 * configuration.setString("key","value");
 * 2、具体参数分为如下三类
 * 1)运行时参数:优化Flink SQL任务在执行时任务性能
 * 2)优化器参数:FlinkSQL任务在生成执行计划时，经过优化器优化生成更优的执行计划
 * 3)表参数:用来调整Flink SQL table的执行行为。
 *
 */
public class ParamsExample {
}
