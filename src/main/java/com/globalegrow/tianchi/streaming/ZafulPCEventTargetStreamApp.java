package com.globalegrow.tianchi.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.AmountModel;
import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.transformation.PHPFilterFunction;
import com.globalegrow.tianchi.transformation.ZafulPCFilterFunction;
import com.globalegrow.tianchi.util.DateUtil;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 19:59 2019/10/31
 * @Modified: 实时统计商品事件数、金额数等
 * 维度：国家 + 平台 + 日期
 * 统计指标(曝光数、点击数、加购数、收藏数、下单数、支付数、支付金额、下单金额)
 */
public class ZafulPCEventTargetStreamApp {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//
        Properties props = new Properties();

        props.setProperty("bootstrap.servers","172.31.35.194:9092,172.31.50.250:9092,172.31.63.112:9092");
        props.setProperty("group.id", "zaful_pc_userinfo");

        //初始化kafka自定义DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>("glbg-analitic-zaful-pc", new SimpleStringSchema(), props);

        FlinkKafkaConsumer011<String> phpKafkaSource =
                new FlinkKafkaConsumer011<>("glbg-analitic-zaful-php", new SimpleStringSchema(), props);

        //获取kafka数据
        DataStreamSource<String> streamPCData = env.addSource(flinkKafkaConsumer011);

        //获取kafka数据
        DataStreamSource<String> streamPHPData = env.addSource(phpKafkaSource);

//        DataStreamSource<String> streamPCData = env.readTextFile("file:///E:\\tmp\\flink\\input\\pc_log.txt");
//
//        DataStreamSource<String> streamPHPData = env.readTextFile("file:///E:\\tmp\\flink\\input\\php_log.txt");

        DataStream<Tuple6<String,String, String,String,String,Integer>> pcResultStream =
                streamPCData.map(new MapFunction<String, PCLogModel>() {
                    @Override
                    public PCLogModel map(String value) throws Exception {

                        PCLogModel pcLogModel = PCFieldsUtils.getPCLogModel(value);

                        return pcLogModel;
                    }
                }).filter(new ZafulPCFilterFunction())
                        .flatMap(new FlatMapFunction<PCLogModel, Tuple6<String,String, String,String,String,Integer>>() {
                            @Override
                            public void flatMap(PCLogModel value, Collector<Tuple6<String,String, String,String,String,Integer>> out) throws Exception {

//                                String cookieId = value.getCookie_id();
//                                String userId = value.getUser_id();
                                String eventType = value.getEvent_type();
                                String timeStamp = value.getUnix_time();

                                String time = null;

                                if (StringUtils.isNotBlank(timeStamp)) {
                                    time = DateUtil.timeStamp2DateStr(timeStamp,"yyyyMMddHH");
                                }

                                String platform = value.getPlatform();
                                String countryCode = value.getCountry_code();

                                //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                                String sub_event_field = value.getSub_event_field();

                                String skuInfo = value.getSkuinfo();

                                List<String> eventFiledSkuList = null;

                                List<String> skuInfoList = null;

                                if (eventType.equals("expose") || eventType.equals("click") ||
                                        eventType.equals("adt") || eventType.equals("collect")){

                                    if (sub_event_field.contains("sku")){
                                        eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);
                                    }

                                    if (skuInfo.contains("sku")){
                                        skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                                    }

                                }

                                try {

                                    if (eventFiledSkuList != null || eventFiledSkuList.size() > 0) { //取sub_event_field里的sku

                                        for (String sku : eventFiledSkuList) {

                                            out.collect(new Tuple6<>(eventType, sku, time,platform,countryCode,1));

//                                            System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);
                                        }

                                    }else {
                                        if (skuInfoList != null || skuInfoList.size() > 0) { //取skuinfo里的sku
                                            for (String sku : skuInfoList) {

                                                out.collect(new Tuple6<>(eventType, sku, time, platform, countryCode, 1));

//                                            System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);
                                            }
                                        }
                                    }
                                }catch (Exception e){
//                                    System.out.println("数据错误：" + e);
                                }
                            }
                        }).keyBy(0,1,2,3,4)
                        .timeWindow(Time.hours(1))
                        .sum(5);
//                        .print().setParallelism(1);

        //处理PHP表数据
        DataStream<Tuple9<String,String,String,String,Double,Integer, String, String, String>> phpResultStream =
                streamPHPData.filter(new PHPFilterFunction())
                        .flatMap(new FlatMapFunction<String, Tuple9<String,String,String,String,Double,Integer, String, String, String>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple9<String,String,String,String,Double,Integer, String, String, String>> out) throws Exception {
                                HashMap<String,Object> dataMap =
                                        JSON.parseObject(value,new TypeReference<HashMap<String,Object>>() {});

                                String cookieId = String.valueOf(dataMap.get("cookie_id"));
                                String userId = String.valueOf(dataMap.get("user_id"));
                                String eventType = String.valueOf(dataMap.get("event_type"));
                                String timeStamp = String.valueOf(dataMap.get("unix_time"));

                                String time = null;

                                if (StringUtils.isNotBlank(timeStamp)){
                                    time = DateUtil.timeStamp2DateStr(timeStamp,"yyyyMMddHH");
                                }

                                String platform = String.valueOf(dataMap.get("platform"));
                                String country_number = String.valueOf(dataMap.get("country_number"));

                                //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                                Object skuInfo =  String.valueOf(dataMap.get("skuinfo"));

                                List<AmountModel> skuInfoList = null;

                                if (String.valueOf(skuInfo).contains("sku")){
                                    skuInfoList = PCFieldsUtils.getSkuAmountFromSkuInfo(skuInfo);
                                }

                                if (eventType.equals("order") || eventType.equals("purchase")){

                                    for (AmountModel sku: skuInfoList){
//                                        System.out.println(cookieId + "\t"+userId+"\t" + eventType + "\t" + sku + "\t" + timeStamp);

                                        out.collect(new Tuple9<>(cookieId,userId,eventType,sku.getSku(),sku.getPrice(),sku.getPam(),time,platform,country_number));
                                    }
                                }
                            }
                        });

        DataStream<Tuple6<String,String, String,String,String,Integer>> phpEventTargetStream =
        phpResultStream.flatMap(new FlatMapFunction<Tuple9<String, String, String, String, Double, Integer, String, String, String>,
                Tuple6<String,String, String,String,String,Integer>>() {
            @Override
            public void flatMap(Tuple9<String, String, String, String, Double, Integer, String, String, String> value,
                                Collector<Tuple6<String,String, String, String, String, Integer>> out) throws Exception {
                out.collect(new Tuple6<>(value.f2,value.f3,value.f6,value.f7,"",1));
            }
        }).keyBy(0,1,2,3,4)
                .timeWindow(Time.hours(1))
                .sum(5);
//        .print();

        DataStream<Tuple6<String,String, String,String,String,Double>> phpOrderTargetStream =
        phpResultStream.flatMap(new FlatMapFunction<Tuple9<String, String, String, String, Double, Integer, String, String, String>,
                Tuple6<String,String, String,String,String,Double>>() {
            @Override
            public void flatMap(Tuple9<String, String, String, String, Double, Integer, String, String, String> value,
                                Collector<Tuple6<String,String, String,String,String,Double>> out) throws Exception {

                String orderGmv = "";

                if (value.f2.equals("order")){
                    orderGmv = "gmv";
                }else {
                    orderGmv = "sales_value";
                }

                out.collect(new Tuple6<>(orderGmv,value.f3,value.f6,value.f7,"",value.f5*value.f4));
            }
        }).keyBy(0,1,2,3,4)
                .timeWindow(Time.hours(1))
                .sum(5);


        DataStream<Tuple6<String,String, String,String,String,Integer>> eventESTargetDS = pcResultStream.union(phpEventTargetStream);

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("100.26.74.93"), 9302));
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("100.26.77.0"), 9302));
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("18.215.206.192"), 9302));


        eventESTargetDS.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<Tuple6<String,String, String,String,String,Integer>>() {
                    public IndexRequest createIndexRequest(Tuple6<String,String, String,String,String,Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("event_type", element.f0);
                        json.put("event_value", element.f1);
                        json.put("time_stamp", element.f2);
                        json.put("platform", element.f3.toLowerCase());
                        json.put("country", element.f4.toUpperCase());
                        json.put("target_value", String.valueOf(element.f5));

                        return Requests.indexRequest().index("zaful_target_event_realtime_pro")
                                .type("ai-zaful-pc-target").source(json);
                    }

                    @Override
                    public void process(Tuple6<String,String, String,String,String,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }));

        phpOrderTargetStream.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<Tuple6<String,String, String,String,String,Double>>() {
                    public IndexRequest createIndexRequest(Tuple6<String,String, String,String,String,Double> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("event_type", element.f0);
                        json.put("event_value", element.f1);
                        json.put("time_stamp", element.f2);
                        json.put("platform", element.f3.toLowerCase());
                        json.put("country", element.f4.toUpperCase());
                        json.put("target_value", String.valueOf(element.f5));

                        return Requests.indexRequest().index("zaful_target_event_realtime_pro")
                                .type("ai-zaful-pc-target").source(json);
                    }

                    @Override
                    public void process(Tuple6<String,String, String,String,String,Double> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }));

        env.execute("ZafulPCEventTargetStreamApp");
    }

}

