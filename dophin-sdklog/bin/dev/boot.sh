#!/bin/bash

#通过脚本路径计算APP相关变量
if readlink -f "$0" > /dev/null 2>&1
then
  SHELL_BIN=$(readlink -f "$0")
else
  SHELL_BIN="$0"
fi

BIN_HOME=$(dirname $SHELL_BIN)
APP_HOME=$BIN_HOME

#打印环境变量
echo "======================================================================================"
echo "SHELL_BIN: "$SHELL_BIN
echo "BIN_HOME:  "$BIN_HOME
echo "APP_HOME:  "$APP_HOME
echo "======================================================================================"

export FLINK_HOME=/opt/flink
export FLINK_CONF_DIR=$APP_HOME/conf

JOB_NAME='sdklog-analisis-streaming'
SAVEPOINT_DIR="hdfs:///user/work/flink/savepoints/"$JOB_NAME

JAR="flink_source_kafka_sink_clickhosue-jar-with-dependencies.jar"
p=3
m=yarn-cluster
yjm=2048
ytm=2048
ys=3
c="com.zhidao.bigdata.plat.dophin.streaming.sdklog.SdkLogStreamingClickHouse"
checkpoint_interval_ms=30000
kafka_source_max_bytes=6097150

start(){
  echo 'starting flink job...'

  # 判断是否存在savepoint
  s=`hadoop fs -ls -t -r $SAVEPOINT_DIR | tail -n 1 | awk -F ' ' '{print $8}'`
  if [ $? -ne 0 ]; then
    echo "find flink job's savepoint failed!!!"
    exit 1
  fi

  cmd="flink run"
  # 如果存在savepoint，则从最新的savepoint启动
  if [ -n "$s" ]; then
    echo "found savepoint: "$s
    cmd=$cmd" -s "$s
  fi
  cmd=$cmd" -p $p -m $m -yqu root.datagroup.rd -ynm $JOB_NAME -yjm $yjm -ytm $ytm -ys $ys"
  cmd=$cmd" -c $c $APP_HOME/$JAR"
  cmd=$cmd" --checkpoint-interval-ms $checkpoint_interval_ms  --kafka-source-max-bytes $kafka_source_max_bytes -d"
  echo "submit flink job with command: '"$cmd"'"

  # 提交flink job
  $cmd &
  sleep 20
  if [ $? -ne 0 ]; then
    echo "flink job submit failed, please check!!!"
    exit 1
  else
    echo "flink job submit success."
    exit 0
  fi
}

stop(){
  echo 'stopping flink job...'

  # 找出yarn application
  application_id=`yarn application -list | grep $JOB_NAME | awk -F ' ' '{print $1}'`
  if [ $? -ne 0 ]; then
    echo "find yarn application failed!!!"
    exit 1
  fi
  echo "yarn application_id is: "$application_id

  # 找出flink job
  job_id=`flink list -yid $application_id | grep '(RUNNING)' | awk -F ':' '{print $4}'| sed 's/^\s*//;s/\s*$//'`
  if [ $? -ne 0 ]; then
    echo "find flink job failed!!!"
    exit 1
  fi
  echo "flink job_id is: "$job_id

  # 停止flink job
  flink stop -yid $application_id -p $SAVEPOINT_DIR $job_id
  if [ $? -ne 0 ]; then
    echo 'stop flink job failed!!!'
    exit 1
  fi

  # 停止yarn application
  yarn application -kill $application_id
  if [ $? -ne 0 ]; then
    echo 'kill yarn application failed!!!'
    exit 1
  fi

  echo 'stopped flink job.'
}

restart(){
  stop
  start
}

case "$1" in
  start)
  start "$2"
  ;;
  stop)
  stop
  ;;
  restart)
  restart
  ;;
  *)
  echo 'Usage command: {start|stop|restart}'
  exit 1
  ;;
esac

exit 0
