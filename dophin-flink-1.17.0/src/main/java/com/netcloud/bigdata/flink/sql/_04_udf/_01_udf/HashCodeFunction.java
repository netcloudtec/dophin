package com.netcloud.bigdata.flink.sql._04_udf._01_udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author netcloud
 * @date 2023-06-09 11:53:50
 * @email netcloudtec@163.com
 * @description
 *
 * 1、类型推导
 * 2、明确UDF的确定性
 * 每个用户自定义函数类都可以通过重写 isDeterministic() 方法来声明它是否产生确定性的结果
 * 如果该函数不是纯粹函数式的（如random(), date(), 或now()），该方法必须返回 false。默认情况下，isDeterministic() 返回 true。
 * 3、巧妙的运用运行时上下文
 * 1)有时候自定义函数需要获取一些全局信息，或者在真正被调用之前做一些配置（setup）/清理（clean-up）的工作。自定义函数也提供了 open() 和 close() 方法，你可以重写这两个方法做到类似于 DataStream API 中 RichFunction 的功能。
 * 2)open() 方法在求值方法被调用之前先调用。close() 方法在求值方法调用完之后被调用。
 * 3)open() 方法提供了一个 FunctionContext，它包含了一些自定义函数被执行时的上下文信息，比如 metric group、分布式文件缓存，或者是全局的作业参数等。
 * 4)下面的信息可以通过调用 FunctionContext 的对应的方法来获得
 *   getMetricGroup():执行该函数的 subtask 的 Metric Group。
 *   getCachedFile(name):分布式文件缓存的本地临时文件副本。
 *   getJobParameter(name, defaultValue):跟对应的 key 关联的全局参数值。
 */
public  class HashCodeFunction extends ScalarFunction {
    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // 获取参数 "hashcode_factor"
        // 如果不存在，则使用默认值 "12"
        factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
    }

    //接收任意类型输入，输出INT类型
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return o.hashCode() * factor;
    }
}
