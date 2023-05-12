package com.zhidao.bigdata.plat.dophin.streaming.sdklog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zhidao.bigdata.plat.dophin.streaming.model.SdkLogDo;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class SdkLogStreamingClickHouse {

    private static final Logger LOGGER = LoggerFactory.getLogger(SdkLogStreamingClickHouse.class);

    private static String bootstrapServers;

    private static String groupId;

    private static String autoOffsetReset;

    private static String coordinateBTopic;

    private static boolean flinkCheckpoint;

    private static String flinkCheckpointDir;

    private static String partitionDiscoveryInterval;

    private static long checkPointTimeout;

    private static long restartBetweenTime;

    private static int restartNum;

    private static long checkpointsBetweenTime;

    private static int maxConcurrentCheckpoints;


    /**
     * batchIntervalMs:写入clickhouse中的每一个批次的时间间隔
     * batchSize:写入clickhosue的每个批次的大小
     * maxRetries:写入clickhouse的重试次数
     */
    private static int batchIntervalMs;
    private static int batchSize;
    private static int maxRetries;

    /**
     * clickhouse的JDBC连接配置
     */
    private static String clickhouseJdbcUrl;
    private static String clickhouseJdbcDriver;

    static {
        Configurations configs = new Configurations();
        try {
            PropertiesConfiguration config = configs.properties(new File("/config/online/application.properties"));
            bootstrapServers = config.getString("sdklog.bootstrap.servers");
            groupId = config.getString("sdklog.group.id");
            autoOffsetReset = config.getString("sdklog.auto.offset.reset");
            coordinateBTopic = config.getString("sdklog.topic");
            flinkCheckpoint = config.getBoolean("flink.checkpoint");
            flinkCheckpointDir = config.getString("flink.checkpoint.dir");
            checkPointTimeout = config.getLong("flink.checkpoint.timeout");
            partitionDiscoveryInterval = config.getString("flink.partition.discovery.interval");
            restartBetweenTime = config.getLong("flink.restart.between.time");
            restartNum = config.getInt("flink.restart.num");
            checkpointsBetweenTime = config.getLong("flink.checkpoints.between.time");
            maxConcurrentCheckpoints = config.getInt("flink.max.concurrent.checkpoints");
            batchIntervalMs = config.getInt("clickhouse.jdbc.batchIntervalMs");
            batchSize = config.getInt("clickhouse.jdbc.batchSize");
            maxRetries = config.getInt("clickhouse.jdbc.maxRetries");
            clickhouseJdbcDriver = config.getString("clickhouse.jdbc.driver");
            clickhouseJdbcUrl = config.getString("clickhouse.jdbc.url");


        } catch (ConfigurationException e) {
            LOGGER.error("read application.properties error..", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {


        //kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", autoOffsetReset);
        // partition discovery
        properties.setProperty("flink.partition-discovery.interval-millis", partitionDiscoveryInterval);

        //传参
        ParameterTool parameters = ParameterTool.fromArgs(args);
        long checkpointIntervalMS = 30000;

        //运行环境并设置checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointIntervalMS);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(checkPointTimeout);
        //设置Flink的重启策略,重试4次,每次间隔1秒
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(restartNum, restartBetweenTime));
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointsBetweenTime);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);

        try {
            env.getConfig().addDefaultKryoSerializer(Class.forName("java.util.Collections$UnmodifiableCollection"), UnmodifiableCollectionsSerializer.class);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

//        if (flinkCheckpoint) {
//            env.setStateBackend(new EmbeddedRocksDBStateBackend());
//            env.getCheckpointConfig().setCheckpointStorage(flinkCheckpointDir);
//            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        }

        //加载kafka数据源并限流
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(coordinateBTopic, new SimpleStringSchema(), properties);

//        FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
//        rateLimiter.setRate(maxBytesPerSecond);
//        consumer.setRateLimiter(rateLimiter);
        DataStream<String> dataStream = env.addSource(consumer).name("SOURCE_SDKLOG_STREAMING");


        String inserSql = "INSERT INTO sdk_log (app_version, scale, fastcgi_script_name, platform, manufacturer" +
                ", http_user_agent, jail_break, time_iso8601, info_list_packageName, info_list_appName" +
                ", system_version, sdk_version, model, sn, brand" +
                ", lat, remote_addr, systemversion, os, lng" +
                ", net_type, data_sources, build_code, upload_time, carrier" +
                ", app_key, info_list_activeTime, package_name, http_x_forwarded_for, imei" +
                ", android_id, channel_id,date,info_list_time,info_list_id,info_list_params,user_id)" +
                "VALUES (?, ?, ?, ?, ?," + "?, ?, ?, ?, ?" + ", ?, ?, ?, ?, ?" +
                ", ?, ?, ?, ?, ?" + ", ?, ?, ?, ?, ?" +
                ", ?, ?, ?, ?, ?,?,?,?,?,?,?,?)";


        JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions
                .builder()
                .withBatchIntervalMs(batchIntervalMs)
                .withBatchSize(batchSize)
                .withMaxRetries(maxRetries)
                .build();

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(clickhouseJdbcUrl)
                .withDriverName(clickhouseJdbcDriver)
                .build();


        SingleOutputStreamOperator<String> filter = dataStream.filter(Objects::nonNull);

        SingleOutputStreamOperator<List<SdkLogDo>> singleOutputStreamOperator = filter.map(new MapFunction<String, List<SdkLogDo>>() {
            @Override
            public List<SdkLogDo> map(String line) throws Exception {
                return parseLine(line);
            }
        });

        singleOutputStreamOperator.flatMap(new FlatMapFunction<List<SdkLogDo>, SdkLogDo>() {
            @Override
            public void flatMap(List<SdkLogDo> sdkLogDos, Collector<SdkLogDo> collector) throws Exception {

                if (sdkLogDos != null) {
                    for (SdkLogDo sdkLogDo : sdkLogDos) {
                        collector.collect(sdkLogDo);
                    }
                }
            }
        }).addSink(
                JdbcSink.sink(inserSql,
                        (ps, sdkLogDo) -> {
                            if (sdkLogDo != null) {
                                try {
                                    ps.setString(1, sdkLogDo.getAppVersion() == null ? "" : sdkLogDo.getAppVersion().trim());
                                    ps.setString(2, sdkLogDo.getScale() == null ? "" : sdkLogDo.getScale());
                                    ps.setString(3, sdkLogDo.getFastcgiScriptName() == null ? "" : sdkLogDo.getFastcgiScriptName());
                                    ps.setInt(4, sdkLogDo.getPlatform() == null ? 0 : sdkLogDo.getPlatform());
                                    ps.setString(5, sdkLogDo.getManufacturer());
                                    ps.setString(6, sdkLogDo.getHttp_user_agent());
                                    ps.setString(7, sdkLogDo.getJail_break());
                                    ps.setString(8, sdkLogDo.getTime_iso8601());
                                    ps.setString(9, sdkLogDo.getInfo_list_packageName());
                                    ps.setString(10, sdkLogDo.getInfo_list_appName());
                                    ps.setString(11, sdkLogDo.getSystem_version());
                                    ps.setString(12, sdkLogDo.getSdk_version());
                                    ps.setString(13, sdkLogDo.getModel());
                                    ps.setString(14, sdkLogDo.getSn());
                                    ps.setString(15, sdkLogDo.getBrand());
                                    ps.setString(16, sdkLogDo.getLat());
                                    ps.setString(17, sdkLogDo.getRemote_addr());
                                    ps.setString(18, sdkLogDo.getSystemversion());
                                    ps.setString(19, sdkLogDo.getOs());
                                    ps.setString(20, sdkLogDo.getLng());
                                    ps.setString(21, sdkLogDo.getNetType());
                                    ps.setString(22, sdkLogDo.getDataSources());
                                    ps.setInt(23, sdkLogDo.getBuildCode() == null ? 0 : sdkLogDo.getBuildCode());
                                    ps.setLong(24, sdkLogDo.getUploadTime() == null ? new java.util.Date().getTime() : sdkLogDo.getUploadTime());
                                    ps.setString(25, sdkLogDo.getCarrier());
                                    ps.setString(26, sdkLogDo.getAppKey());
                                    ps.setLong(27, sdkLogDo.getInfoListActiveTime() == null ? 0 : sdkLogDo.getInfoListActiveTime());
                                    ps.setString(28, sdkLogDo.getPackageName() == null ? "" : sdkLogDo.getPackageName());
                                    ps.setString(29, sdkLogDo.getHttpXForwardedFor());
                                    ps.setString(30, sdkLogDo.getImei());
                                    ps.setString(31, sdkLogDo.getAndroidId());
                                    ps.setString(32, sdkLogDo.getChannelId());
                                    ps.setDate(33, sdkLogDo.getDate());

                                    if (sdkLogDo.getInfo_list_time() != null) {
                                        ps.setLong(34, sdkLogDo.getInfo_list_time());
                                    } else {
                                        ps.setLong(34, 0);
                                    }

                                    if (sdkLogDo.getInfo_list_id() != null) {
                                        ps.setString(35, sdkLogDo.getInfo_list_id());
                                    } else {
                                        ps.setNull(35, Types.VARCHAR);
                                    }

                                    if (sdkLogDo.getInfo_list_params() != null) {
                                        ps.setString(36, sdkLogDo.getInfo_list_params());
                                    } else {
                                        ps.setNull(36, Types.VARCHAR);
                                    }

                                    if (sdkLogDo.getUserId() != null) {
                                        ps.setString(37, sdkLogDo.getUserId());
                                    } else {
                                        ps.setNull(37, Types.VARCHAR);
                                    }

                                } catch (Exception e) {
                                    LOGGER.error(e.getMessage(), e);
                                }
                            }
                        },

                        jdbcExecutionOptions
                        ,
                        jdbcConnectionOptions
                )).name("sink jdbc to clickhosue");

        try {
            env.execute(SdkLogStreamingClickHouse.class.getCanonicalName());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static List<SdkLogDo> parseLine(String line) {

        List<SdkLogDo> sdkLogDos = Lists.newArrayList();

        String[] splitLine = line.split("\\| \\|");

        String fastcgi_script_name = "";

        if (splitLine[0].split("/").length > 1) {
            fastcgi_script_name = "/" + splitLine[0].split("/")[1];
        } else {
            LOGGER.error("fastcgi_script_name split之前====={}", splitLine[0]);
        }

        String time_iso8601 = null;
        String http_x_forwarded_for = null;
        String request_body = null;
        String http_user_agent = null;
        String remote_addr = null;
        String logsdkid = null;
        String info_list = null;
        JSONArray jsonArray = null;
        JSONObject jsonObject = null;

        boolean flag = false;
        try {
            time_iso8601 = splitLine[1];
            http_x_forwarded_for = splitLine[2];
            request_body = splitLine[3];
            http_user_agent = splitLine[4];
            remote_addr = splitLine[5];

            //过滤一些infolist不是json的数据

            jsonObject = JSON.parseObject(request_body);
            info_list = jsonObject.getString("info_list");
            jsonArray = JSON.parseArray(info_list);
            flag = true;
        } catch (Exception e) {
            return sdkLogDos;
        }

        if (flag) {
            if (jsonArray != null) {

                for (int i = 0; i < jsonArray.size(); i++) {

                    Map mapSdkLog = new HashMap();

                    String stringOne = jsonArray.getString(i);

                    Map<String, String> mapInfo = JSON.parseObject(stringOne, Map.class);

                    try {
                        mapSdkLog = JSON.parseObject(request_body, Map.class);
                    } catch (Exception e) {
                        LOGGER.warn("request_body is not json" + request_body);
                    }

                    if (request_body.contains("info_list")) {
                        mapSdkLog.remove("info_list");
                    }

                    for (Map.Entry<String, String> entry : mapInfo.entrySet()) {
                        mapSdkLog.put("info_list_" + entry.getKey(), entry.getValue());
                    }

                    mapInfo.clear();
                    mapSdkLog.put("fastcgi_script_name", fastcgi_script_name);

                    mapSdkLog.put("time_iso8601", time_iso8601);
                    mapSdkLog.put("http_x_forwarded_for", http_x_forwarded_for);
                    mapSdkLog.put("http_user_agent", http_user_agent);
                    mapSdkLog.put("remote_addr", remote_addr);

                    if (splitLine.length == 7) {
                        mapSdkLog.put("logsdkid", logsdkid);
                    }

                    /**
                     * 将mapSdkLog转为SdkLogDo对象
                     */
                    sdkLogDos.add(mapToSdkLogDo(mapSdkLog));
                }

            } else {
                /**
                 * 这里过滤掉一些数据，如果 info_list中没有数据的
                 */
                return null;
            }

        }


        return sdkLogDos;
    }


    public static SdkLogDo mapToSdkLogDo(Map map) {

        SdkLogDo sdkLogDo = new SdkLogDo();

        try {

            sdkLogDo.setAppVersion(map.get("app_version") != null ? (String) map.get("app_version") : null);
            sdkLogDo.setScale(map.get("scale") != null ? (String) map.get("scale") : null);
            sdkLogDo.setFastcgiScriptName(map.get("fastcgi_script_name") != null ? (String) map.get("fastcgi_script_name") : null);

            if (map.get("platform") instanceof Integer) {
                sdkLogDo.setPlatform(map.get("platform") != null ? (Integer) map.get("platform") : null);
            }

            if (map.get("platform") instanceof String) {
                sdkLogDo.setPlatform(map.get("platform") != null ? Integer.parseInt((String) map.get("platform")) : null);
            }
            sdkLogDo.setPlatform(map.get("platform") != null ? (Integer) map.get("platform") : null);

            sdkLogDo.setManufacturer(map.get("manufacturer") != null ? (String) map.get("manufacturer") : null);
            sdkLogDo.setHttp_user_agent(map.get("http_user_agent") != null ? (String) map.get("http_user_agent") : null);

            if (map.get("jail_break") instanceof Integer) {
                sdkLogDo.setJail_break(map.get("jail_break") != null ? String.valueOf((Integer) map.get("jail_break")) : null);
            }
            if (map.get("jail_break") instanceof String) {
                sdkLogDo.setJail_break(map.get("jail_break") != null ? String.valueOf((String) map.get("jail_break")) : null);
            }

            sdkLogDo.setTime_iso8601(map.get("time_iso8601") != null ? (String) map.get("time_iso8601") : null);

            sdkLogDo.setInfo_list_packageName(map.get("info_list_packageName") != null ? (String) map.get("info_list_packageName") : null);
            sdkLogDo.setSystem_version(map.get("system_version") != null ? (String) map.get("system_version") : null);
            sdkLogDo.setSdk_version(map.get("sdk_version") != null ? (String) map.get("sdk_version") : null);
            sdkLogDo.setModel(map.get("model") != null ? (String) map.get("model") : null);

            sdkLogDo.setSn(map.get("sn") != null ? (String) map.get("sn") : null);

            sdkLogDo.setBrand(map.get("brand") != null ? (String) map.get("brand") : null);

            if (map.get("lat") instanceof BigDecimal) {
                sdkLogDo.setLat(map.get("lat") != null ? map.get("lat").toString() : null);
            }

            if (map.get("lat") instanceof String) {
                sdkLogDo.setLat(map.get("lat") != null ? (String) map.get("lat") : null);
            }

            sdkLogDo.setRemote_addr(map.get("remote_addr") != null ? (String) map.get("remote_addr") : null);
            sdkLogDo.setSystemversion(map.get("systemversion") != null ? (String) map.get("systemversion") : null);
            sdkLogDo.setOs(map.get("os") != null ? (String) map.get("os") : null);


            if (map.get("lng") instanceof Integer) {
                sdkLogDo.setLng(map.get("lng") != null ? String.valueOf((Integer) map.get("lng")) : null);
            }
            if (map.get("lng") instanceof String) {
                sdkLogDo.setLng(map.get("lng") != null ? (String) map.get("lng") : null);
            }

            sdkLogDo.setNetType(map.get("net_type") != null ? (String) map.get("net_type") : null);
            sdkLogDo.setDataSources(map.get("data_sources") != null ? (String) map.get("data_sources") : null);

            if (map.get("build_code") instanceof String) {
                sdkLogDo.setBuildCode(map.get("build_code") != null ? Integer.parseInt((String) map.get("build_code")) : null);
            }

            if (map.get("build_code") instanceof Integer) {
                sdkLogDo.setBuildCode(map.get("build_code") != null ? (Integer) map.get("build_code") : null);
            }


            if (map.get("upload_time") instanceof String) {
                sdkLogDo.setUploadTime(map.get("upload_time") != null ? Long.parseLong((String) map.get("upload_time")) : null);
            }
            if (map.get("upload_time") instanceof Long) {
                sdkLogDo.setUploadTime(map.get("upload_time") != null ? (Long) map.get("upload_time") : null);
            }

            sdkLogDo.setCarrier(map.get("carrier") != null ? (String) map.get("carrier") : null);
            sdkLogDo.setAppKey(map.get("app_key") != null ? (String) map.get("app_key") : null);


            if (map.get("info_list_activeTime") instanceof Long) {
                sdkLogDo.setInfoListActiveTime(map.get("info_list_activeTime") != null ? (Long) map.get("info_list_activeTime") : null);
            }

            sdkLogDo.setPackageName(map.get("package_name") != null ? (String) map.get("package_name") : null);
            sdkLogDo.setHttpXForwardedFor(map.get("http_x_forwarded_for") != null ? (String) map.get("http_x_forwarded_for") : null);
            sdkLogDo.setImei(map.get("imei") != null ? (String) map.get("imei") : null);
            sdkLogDo.setAndroidId(map.get("android_id") != null ? (String) map.get("android_id") : null);
            sdkLogDo.setChannelId(map.get("channel_id") != null ? (String) map.get("channel_id") : null);

            final String ISO_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
            SimpleDateFormat dateFormat = new SimpleDateFormat(ISO_PATTERN);

            sdkLogDo.setDate(sdkLogDo.getTime_iso8601() != null ? new Date(dateFormat.parse(sdkLogDo.getTime_iso8601()).getTime()) : new Date(new java.util.Date().getTime()));


            if (map.get("info_list_params") != null) {
                sdkLogDo.setInfo_list_params((String) map.get("info_list_params"));
            }
            if (map.get("info_list_id") != null) {
                sdkLogDo.setInfo_list_id((String) map.get("info_list_id"));
            }

            if (map.get("info_list_time") != null) {
                if (map.get("info_list_time") instanceof Long) {
                    sdkLogDo.setInfo_list_time((Long) map.get("info_list_time"));
                }
                if (map.get("info_list_time") instanceof Integer) {
                    sdkLogDo.setInfo_list_time(((Integer) map.get("info_list_time")).longValue());
                }
            }

            if (map.get("user_id") != null) {
                if (map.get("user_id") instanceof String) {
                    sdkLogDo.setUserId((String) map.get("user_id"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sdkLogDo;
    }

}
