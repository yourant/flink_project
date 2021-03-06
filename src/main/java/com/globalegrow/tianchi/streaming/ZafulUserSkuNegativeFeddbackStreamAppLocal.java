package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.transformation.ZafulPCEventFilterFunction;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 16:23 2019/10/31
 * @Modified:
 */
public class ZafulUserSkuNegativeFeddbackStreamAppLocal {
    protected static final Logger logger = LoggerFactory.getLogger(ZafulUserSkuNegativeFeddbackStreamAppLocal.class);


    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String redisServers = "192.168.211.200:6380,192.168.211.200:6381,192.168.211.201:6380,192" +
                ".168.211.201:6381,192.168.211.202:6380,192.168.211.202:6381";

        String redisPassword = "6e1KWyC29w";


        String keyPrefix = "zaful_pc_negative_feedback_";

        Integer expireSeconds = 86400;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        DataStreamSource<String> streamPCData = env.readTextFile("C:\\Users\\Administrator" +
                "\\Desktop\\pc_log.txt");

        SingleOutputStreamOperator<PCLogModel> burialAfterEventFilter = streamPCData.map(new MapFunction<String, PCLogModel>() {
            @Override
            public PCLogModel map(String value) throws Exception {

                PCLogModel pcLogModel = PCFieldsUtils.getPCLogModel(value);

                return pcLogModel;
            }
        }).uid("conver_burail_to_model").filter(new FilterFunction<PCLogModel>() {
            @Override
            public boolean filter(PCLogModel pcLogModel) throws Exception {
                boolean filter = false;
                if (pcLogModel != null) {
                     filter = true;
                }
                return filter;
            }
        }).filter(new ZafulPCEventFilterFunction()).uid("event_filter");


        SingleOutputStreamOperator<Tuple3<String, String, String>> burialAfterConvert =
                burialAfterEventFilter.flatMap(new FlatMapFunction<PCLogModel, Tuple3<String, String, String>>() {
                    @Override
                    public void flatMap(PCLogModel value, Collector<Tuple3<String, String, String>> out) throws Exception {

                        String cookieId = value.getCookie_id();
                        String eventType = value.getEvent_type();
                        if (StringUtils.isNotBlank(cookieId)) {

                            //???skuinfo???sub_event_field???sku????????????????????????json??????????????????????????????json??????
                            String sub_event_field = value.getSub_event_field();
                            String skuInfo = value.getSkuinfo();
                            List<String> eventFiledSkuList = null;
                            List<String> skuInfoList = null;
                            //??????sub_event_field?????????????????????????????????skuinfo???????????????
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
                                logger.error("?????? json ????????????: {}", value, e);
                            }
                        }
                    }
                }).uid("convert_date_to_redis_local");
         burialAfterConvert.writeAsText("C:\\Users\\Administrator\\Desktop\\pc_log11.txt", FileSystem.WriteMode.OVERWRITE);
      //  burialAfterConvert.writeUsingOutputFormat(new RedisLPushAndLTrimOutPut(redisServers,
             //   redisPassword, keyPrefix, expireSeconds));

        env.execute(parameterTool.get("job-name",
                "realtime-user-event-negative-feddback-redis-local") + System.currentTimeMillis());
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
            System.out.println("---configure");
            String[] serverArray = this.redisServers.split(",");
            Set<HostAndPort> nodes = new HashSet<>();

            for (String ipPort : serverArray) {
                String[] ipPortPair = ipPort.split(":");
                nodes.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
            }


            this.jedisCluster = new JedisCluster(nodes, 10000, 10000, 2,
                    this.redisPassword, new GenericObjectPoolConfig());

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
                String sku = record.getField(2).toString();
                String redisKey = this.keyPrefix + cookieId;
                if ("expose".equalsIgnoreCase(eventType)) {
                    this.jedisCluster.zincrby(redisKey, 1, sku);
                    this.jedisCluster.expire(redisKey, this.expireSeconds);
                } else {
                    this.jedisCluster.zadd(redisKey, -5, sku);
                    this.jedisCluster.expire(redisKey, this.expireSeconds);
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("??????redis??????: {}", record, e);
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
