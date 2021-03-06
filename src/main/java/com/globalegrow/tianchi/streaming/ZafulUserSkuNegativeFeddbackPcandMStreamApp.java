package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.transformation.ZafulPCEventFilterFunction;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 用户负反馈spu计算flink流
 *
 * @Author: joker
 * @Description:
 * @Date: Created in 16:23 2019/10/31
 * @Modified: /usr/local/services/flink-1.9.0/bin/flink run -d -m yarn-cluster -yn 3 -yjm 1024 -ytm 1024 -yqu root.flink -ynm
 * UserSkuNegativeFeddback -c com.globalegrow.tianchi.streaming.ZafulUserSkuNegativeFeddbackPcandMStreamApp
 * /home/user_00/flink_project-1.0-jar-with-dependencies.jar --parallelism 3 --checkpoit 500 --bootstrap.servers
 * 172.31.35.194:9092,172.31.50.250:9092,172.31.63.112:9092,172.31.53.157:9092,172.31.54.122:9092 --group.id
 * UserSkuNegativeFeddback——20191118714 --topic glbg-analitic-zaful-pc --startFromEarliest false --redisServers
 * arkpipeline-0001-001.mczgdb.0001.use1.cache.amazonaws.com:6379 --redisPassword
 * 6e1KWyC29w --keyPrefix zaful_pc_negative_feedback_ww --expireSeconds 86400
 */
public class ZafulUserSkuNegativeFeddbackPcandMStreamApp {
    protected static final Logger logger = LoggerFactory.getLogger(ZafulUserSkuNegativeFeddbackPcandMStreamApp.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String redisServers = parameterTool.getRequired("redisServers");

        String redisPassword = parameterTool.getRequired("redisPassword");


        String keyPrefix = parameterTool.getRequired("keyPrefix");

        Integer expireSeconds = parameterTool.getInt("expireSeconds", 86400);

        //并行度
        String parallelism = parameterTool.getRequired("parallelism");

        //checkpoit
        String checkpoit = parameterTool.getRequired("checkpoit");

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(Integer.valueOf(parallelism));

        //设置checkpoit
        env.enableCheckpointing(Long.valueOf(checkpoit));

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        props.setProperty("group.id", parameterTool.getRequired("group.id"));

        //初始化kafka自定义DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), props);
        Boolean startFromEarliest = parameterTool.getBoolean("startFromEarliest", false);
        if (startFromEarliest) {
            flinkKafkaConsumer011.setStartFromEarliest();
        }

        //获取kafka数据
        DataStream<String> streamPCData = env.addSource(flinkKafkaConsumer011).shuffle();

        SingleOutputStreamOperator<PCLogModel> burialAfterEventFilter = streamPCData.map(new MapFunction<String, PCLogModel>() {
            @Override
            public PCLogModel map(String value) throws Exception {

                PCLogModel pcLogModel = null;

                try {

                    pcLogModel = PCFieldsUtils.getPCLogModel(value);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return pcLogModel;
            }
        }).uid("conver_burail_to_model").filter(new ZafulPCEventFilterFunction()).uid("event_filter");


        SingleOutputStreamOperator<Tuple3<String, String, String>> burialAfterConvert =
                burialAfterEventFilter.flatMap(new FlatMapFunction<PCLogModel, Tuple3<String, String, String>>() {
                    @Override
                    public void flatMap(PCLogModel value, Collector<Tuple3<String, String, String>> out) throws Exception {
                        try {
                            if (value != null) {

                                String cookieId = value.getCookie_id();
                                String eventType = value.getEvent_type();
                                if (StringUtils.isNotBlank(cookieId)) {


                                    //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                                    String sub_event_field = value.getSub_event_field();
                                    String skuInfo = value.getSkuinfo();
                                    List<String> eventFiledSkuList = null;

                                    List<String> skuInfoList = null;
                                    //先从sub_event_field里面拿数据，拿不到才从skuinfo里面拿数据
                                    try {
                                        if (StringUtils.isNotBlank(sub_event_field) && sub_event_field.contains("sku")) {
                                            eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);

                                        }
                                        if (eventFiledSkuList != null && eventFiledSkuList.size() > 0) {

                                            for (String sku : eventFiledSkuList) {

                                                out.collect(new Tuple3<>(cookieId, eventType, sku));
                                            }

                                        } else if (StringUtils.isNotBlank(skuInfo) && skuInfo.contains("sku")) {
                                            skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                                            if (skuInfoList != null && skuInfoList.size() > 0) {
                                                for (String sku : skuInfoList) {
                                                    out.collect(new Tuple3<>(cookieId, eventType, sku));
                                                }
                                            }
                                        }

                                    } catch (Exception e) {
                                        logger.error("解析 json 数据出错: {}", value, e);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }).uid("convert_date_to_redis");
        burialAfterConvert.writeUsingOutputFormat(new

                RedisLPushAndLTrimOutPut(redisServers, redisPassword, keyPrefix, expireSeconds));

        env.execute(parameterTool.get("job-name", "realtime-user-event-negative-feddback-redis") + System.currentTimeMillis());
    }

    static class RedisLPushAndLTrimOutPut implements OutputFormat<Tuple3<String, String, String>> {

        private JedisCluster jedisCluster;

        private String redisServers;

        private String redisPassword;


        private String keyPrefix;

        private Integer expireSeconds;


        public RedisLPushAndLTrimOutPut(String redisServers, String redisPassword, String keyPrefix, Integer expireSeconds) {
            this.redisServers = redisServers;
            this.redisPassword = redisPassword;
            this.keyPrefix = keyPrefix;
            this.expireSeconds = expireSeconds;
        }

        /**
         * Configures this output format. Since output formats are instantiated generically and hence parameterless,
         * this method is the place where the output formats set their basic fields based on configuration values.
         * <p>
         * This method is always called first on a newly instantiated output format.
         *
         * @param parameters The configuration with all parameters.
         */
        @Override
        public void configure(Configuration parameters) {
            logger.info("configure");
            String[] serverArray = this.redisServers.split(",");
            Set<HostAndPort> nodes = new HashSet<>();

            for (String ipPort : serverArray) {
                logger.info("ipPort{}", ipPort);
                System.out.println("ipPort-----------" + ipPort);
                String[] ipPortPair = ipPort.split(":");
                nodes.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
            }

//            this.jedisCluster = new JedisCluster(nodes, 10000, 10000, 2,this.redisPassword, new GenericObjectPoolConfig());

            this.jedisCluster = new JedisCluster(nodes, 10000, 10000, 2, new GenericObjectPoolConfig());

        }

        /**
         * Opens a parallel instance of the output format to store the result of its parallel instance.
         * <p>
         * When this method is called, the output format it guaranteed to be configured.
         *
         * @param taskNumber The number of the parallel instance.
         * @param numTasks   The number of parallel tasks.
         * @throws IOException Thrown, if the output could not be opened due to an I/O problem.
         */
        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            logger.info("open");
        }

        /**
         * Adds a record to the output.
         * <p>
         * When this method is called, the output format it guaranteed to be opened.
         *
         * @param record The records to add to the output.
         * @throws IOException Thrown, if the records could not be added to to an I/O problem.
         */
        @Override
        public void writeRecord(Tuple3<String, String, String> record) throws IOException {

            try {
                String cookieId = record.getField(0).toString();
                String eventType = record.getField(1).toString();
                //String sku = record.getField(2).toString();
                String spu = record.getField(2).toString().substring(0, 7);
                String redisKey = this.keyPrefix + cookieId;
                if ("expose".equalsIgnoreCase(eventType)) {
                    this.jedisCluster.zincrby(redisKey, 1, spu);
                    this.jedisCluster.expire(redisKey, this.expireSeconds);
                } else {
                    this.jedisCluster.zadd(redisKey, -5, spu);
                    this.jedisCluster.expire(redisKey, this.expireSeconds);
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("写入redis出错: {}", record, e);
            }

        }

        /**
         * Method that marks the end of the life-cycle of parallel output instance. Should be used to close
         * channels and streams and release resources.
         * After this method returns without an error, the output is assumed to be correct.
         * <p>
         * When this method is called, the output format it guaranteed to be opened.
         *
         * @throws IOException Thrown, if the input could not be closed properly.
         */
        @Override
        public void close() throws IOException {
            logger.info("close");
            this.jedisCluster.close();
        }
    }
}