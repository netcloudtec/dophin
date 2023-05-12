#!/bin/bash

#计算脚本所在目录
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
if readlink -f "$BASE_DIR" > /dev/null 2>&1; then
    BASE_DIR=$(readlink -f "$BASE_DIR")
fi

#target目录
TARGET_PATH=$BASE_DIR/target
SERVICE_NAME="dophin-sdklog"
TARGET_JAR=$SERVICE_NAME"-jar-with-dependencies.jar"
TARGET_PATH_JAR=$TARGET_PATH/$TARGET_JAR
OUTPUT_PATH=$BASE_DIR/output

#设置profile
profile="online"
if [ $# -ge 1 ]; then
    profile="$1"
fi

echo "======================================================================================"
echo "PROFILE: $profile"
echo "HOME: $BASE_DIR"
echo "======================================================================================"

#开始打包
echo "NOW, compile ..."
mvn clean package -DskipTests=true -P$profile -f ../pom.xml -pl dophin-sdklog -am

if [ $? -ne 0 ]; then
 echo "build failed."
 exit 1
fi

rm -rf $OUTPUT_PATH
mkdir -p $OUTPUT_PATH
cp $TARGET_PATH_JAR $OUTPUT_PATH
cp -r $BASE_DIR/bin/$profile/* $OUTPUT_PATH

mkdir -p $OUTPUT_PATH/conf
cp -r $BASE_DIR/conf/$profile/* $OUTPUT_PATH/conf/

echo "build complete."
exit 0