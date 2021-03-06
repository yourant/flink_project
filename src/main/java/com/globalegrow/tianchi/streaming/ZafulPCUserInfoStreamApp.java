package com.globalegrow.tianchi.streaming;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.bean.SkuInfo;
import com.globalegrow.tianchi.bean.SubEventField;
import com.globalegrow.tianchi.bean.ZafulPCUserInfo;
import com.globalegrow.tianchi.transformation.PHPFilterFunction;
import com.globalegrow.tianchi.transformation.ZafulPCFilterFunction;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 9:42 2019/10/31
 * @Modified:
 */
public class ZafulPCUserInfoStreamApp {

    private static Logger logger = LoggerFactory.getLogger(ZafulPCUserInfoStreamApp.class);

    public static void main(String[] args) throws Exception{

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //?????????
        String parallelism = parameterTool.getRequired("parallelism");

        //checkpoit
        String checkpoit = parameterTool.getRequired("checkpoit");

        //??????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //?????????????????????
        env.setParallelism(Integer.valueOf(parallelism));

        //??????checkpoit
        env.enableCheckpointing(Long.valueOf(checkpoit));

        Properties props = new Properties();

        props.setProperty("bootstrap.servers",parameterTool.getRequired("bootstrap.servers"));
        props.setProperty("group.id", parameterTool.getRequired("group.id"));

        //?????????kafka?????????DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), props);
        Boolean startFromEarliest = parameterTool.getBoolean("startFromEarliest", false);
        if (startFromEarliest) {
            flinkKafkaConsumer011.setStartFromEarliest();
        }

        //??????kafka??????
        DataStream<String> streamPCData = env.addSource(flinkKafkaConsumer011).shuffle();

        //??????PC?????????
        DataStream<Tuple5<String,String, String, String, String>> pcResultStream =
                streamPCData.map(new MapFunction<String, PCLogModel>() {
                    @Override
                    public PCLogModel map(String value) throws Exception {

                        PCLogModel pcLogModel = null;

                        try {

                            pcLogModel = PCFieldsUtils.getPCLogModel(value);
                        }catch (Exception e){
                            e.printStackTrace();
                        }

                        return pcLogModel;
                    }
                }).uid("zaful_pc_data_map").filter(new ZafulPCFilterFunction()).uid("zaful_pc_data_filter")
                .flatMap(new FlatMapFunction<PCLogModel, Tuple5<String,String, String, String, String>>() {
                    @Override
                    public void flatMap(PCLogModel value, Collector<Tuple5<String,String, String, String, String>> out) throws Exception {

                        try {

                        if (value != null) {

                            String cookieId = value.getCookie_id();
                            String userId = value.getUser_id();
                            String eventType = value.getEvent_type();
                            String timeStamp = value.getUnix_time();

                            //???skuinfo???sub_event_field???sku????????????????????????json??????????????????????????????json??????
                            String sub_event_field = value.getSub_event_field();
                            String skuInfo = value.getSkuinfo();

                            if (eventType.equals("search")) {
                                String searchWord = value.getSearch_result_word();
                                if (StringUtils.isBlank(searchWord)){
                                    searchWord = value.getPage_code().replace("s-","");
                                }
                                out.collect(new Tuple5<>(cookieId, userId, eventType, searchWord, timeStamp));
                            }

                            List<String> eventFiledSkuList = null;

                            List<String> skuInfoList = null;

                            if (eventType.equals("expose") || eventType.equals("click") ||
                                    eventType.equals("adt") || eventType.equals("collect")) {

                                if (StringUtils.isNotBlank(sub_event_field) && sub_event_field.contains("sku")) {
                                    eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);
                                }
                            }

                            try {

                                if (eventFiledSkuList != null && eventFiledSkuList.size() > 0) {

                                    for (String sku : eventFiledSkuList) {

                                        out.collect(new Tuple5<>(cookieId, userId, eventType, sku, timeStamp));
                                    }

                                } else if (StringUtils.isNotBlank(skuInfo) && skuInfo.contains("sku")) {
                                    skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                                    if (skuInfoList != null && skuInfoList.size() > 0) {
                                        for (String sku : skuInfoList) {
                                            out.collect(new Tuple5<>(cookieId, userId, eventType, sku, timeStamp));
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                System.out.println("???????????????" + e);
                            }
                        }
                        }catch (Exception e){
                            e.printStackTrace();
                        }

                    }
                }).uid("pc_clean_data_flatmap");

        //??????????????????ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        pcResultStream.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<Tuple5<String,String,String,String,String>>() {
            public IndexRequest createIndexRequest(Tuple5<String,String,String,String,String> element) {

                Map<String, Object> json = new HashMap<>();
                json.put("cookie_id", element.f0);
                json.put("user_id", element.f1);
                json.put("event_type", element.f2);
                json.put("event_value", element.f3);
//                long timeStamp = Long.valueOf(element.f4.substring(0,element.f4.length()-3));
                json.put("time_stamp", Long.valueOf(element.f4));
//
                return Requests.indexRequest().index("zaful"+"_"+"user"+"_"+element.f2+"_event_realtime")
                        .type("ai-zaful-pc-userinfo").routing(element.f0).source(json);
            }

            @Override
            public void process(Tuple5<String,String,String,String,String> element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        })).name("zaful_pc_userInfo_sink_es");

        env.execute("ZafulPCUserInfoStreamApp" + System.currentTimeMillis());
    }
}
