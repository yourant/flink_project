package com.globalegrow.tianchi.streaming.local;

import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.transformation.ZafulPCEventFilterFunction;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * @Description Zaful PCM各页面商品维度的实时统计数据
 * 项目：AI指挥官项目
 * 背景：对于zaful站点pc端的商品扶持，需要实时的商品数据，尽早选择出爆款商品，进行大流量扶持，一方面可以提高zaful的转化率，提高千次曝光GMV；另一方面，可以打通供应链，降低成本和回货周期，获得效率提升和利润提高。
 * wiki：http://wiki.hqygou.com:8090/pages/viewpage.action?pageId=186614393
 * @Author chongzi
 * @Date 2019/11/25 10:02
 **/
public class ZafulPcmGoodPageEventCountAppLocal {

    protected static final Logger logger = LoggerFactory.getLogger(ZafulPcmGoodPageEventCountAppLocal.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String redisHost = "127.0.0.1";

        Integer redisPort = 6379;

        String keyPrefix = "zaful_goods_page_count_";

        Integer expireSeconds = 86400;

        //并行度
        Integer parallelism = 1;

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度
        env.setParallelism(parallelism);


        DataStream<String> text = env.readTextFile("D://projects//flink_project//src//main//resources//data//ZafulPcmGoodPageEventCountAppData.txt");
        SingleOutputStreamOperator<PCLogModel> burialAfterEventFilter = text.map(new MapFunction<String, PCLogModel>() {
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
                }).uid("convert_date_to_redis_local");

        burialAfterConvert.addSink(new SinkToRedis(redisHost, redisPort, keyPrefix, expireSeconds));

        env.execute(parameterTool.get("job-name", "ZafulPcmGoodPageEventCountAppLocal") + System.currentTimeMillis());
    }

    public static class SinkToRedis extends RichSinkFunction<Tuple3<String, String, String>> {

        private Jedis jedis = null;

        private JedisPoolConfig config = null;

        private JedisPool pool = null;

        private String redisHost;

        private Integer redisPort;


        private String keyPrefix;

        private Integer expireSeconds;

        public SinkToRedis(String redisHost, Integer redisPort, String keyPrefix, Integer expireSeconds) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
            this.keyPrefix = keyPrefix;
            this.expireSeconds = expireSeconds;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            logger.info("in open");
            super.open(parameters);

            config = new JedisPoolConfig();

            config.setMaxTotal(500);

            config.setMaxIdle(5);

            config.setMaxWaitMillis(1000 * 3600);

            config.setTestOnBorrow(true);
            config = new JedisPoolConfig();
            pool = new JedisPool(config, this.redisHost, this.redisPort, 20000);
            jedis = pool.getResource();
        }


        @Override
        public void invoke(Tuple3<String, String, String> record, Context context) throws Exception {
            logger.info("in invoke");
            try {
                String cookieId = record.getField(0).toString();
                String eventType = record.getField(1).toString();
                String sku = record.getField(2).toString();
                String redisKey = this.keyPrefix + cookieId;
                if ("expose".equalsIgnoreCase(eventType)) {
                    this.jedis.zincrby(redisKey, 1, sku);
                    this.jedis.expire(redisKey, this.expireSeconds);
                } else {
                    this.jedis.zadd(redisKey, -5, sku);
                    this.jedis.expire(redisKey, this.expireSeconds);
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("写入redis出错: {}", record, e);
            }
        }

        @Override
        public void close() throws Exception {
            logger.info("in close");
            super.close();
            if (this.jedis != null) {
                this.jedis.close();
            }
        }
    }
}