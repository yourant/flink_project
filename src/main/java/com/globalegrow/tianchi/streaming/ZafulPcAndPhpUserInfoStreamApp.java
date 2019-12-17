package com.globalegrow.tianchi.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.transformation.PHPFilterFunction;
import com.globalegrow.tianchi.transformation.ZafulPCFilterFunction;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 17:27 2019/11/18
 * @Modified:
 */
public class ZafulPcAndPhpUserInfoStreamApp {

    public static void main(String[] args) throws Exception{

//        获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //处理PC表数据
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
                }).filter(new ZafulPCFilterFunction())
                        .flatMap(new FlatMapFunction<PCLogModel, Tuple5<String,String, String, String, String>>() {
                            @Override
                            public void flatMap(PCLogModel value, Collector<Tuple5<String,String, String, String, String>> out) throws Exception {

                                try {

                                    if (value != null) {

                                        String cookieId = value.getCookie_id();
                                        String userId = value.getUser_id();
                                        String eventType = value.getEvent_type();
                                        String timeStamp = value.getUnix_time();

                                        //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                                        String sub_event_field = value.getSub_event_field();
                                        String skuInfo = value.getSkuinfo();

                                        if (eventType.equals("search")) {
                                            out.collect(new Tuple5<>(cookieId, userId, eventType, value.getSearch_result_word(), timeStamp));
                                        }

                                        List<String> eventFiledSkuList = null;

                                        List<String> skuInfoList = null;

                                        if (eventType.equals("expose") || eventType.equals("click") ||
                                                eventType.equals("adt") || eventType.equals("collect")) {

                                            if (sub_event_field.contains("sku")) {
                                                eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);
                                            }

                                            if (skuInfo.contains("sku")) {
                                                skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                                            }

                                        }

                                        if (StringUtils.isNotBlank(sub_event_field) && null != sub_event_field) {
                                            eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);
                                        }

                                        try {

                                            if (eventFiledSkuList != null && eventFiledSkuList.size() > 0) {

                                                for (String sku : eventFiledSkuList) {

                                                    out.collect(new Tuple5<>(cookieId, userId, eventType, sku, timeStamp));

                                                }

                                            } else {
                                                if (skuInfoList != null && skuInfoList.size() > 0) {
                                                    for (String sku : skuInfoList) {

                                                        out.collect(new Tuple5<>(cookieId, userId, eventType, sku, timeStamp));

                                                    }
                                                }
                                            }
                                        } catch (Exception e) {
                                            System.out.println("数据错误：" + e);
                                        }
                                    }
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                            }
                        }).setParallelism(3);

        //处理PHP表数据
        DataStream<Tuple5<String,String,String,String,String>> phpResultStream =
                streamPHPData.filter(new PHPFilterFunction())
                        .flatMap(new FlatMapFunction<String, Tuple5<String,String,String,String,String>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple5<String,String, String, String, String>> out) throws Exception {

                                try {

                                    if (StringUtils.isNotBlank(value)) {

                                        HashMap<String, Object> dataMap =
                                                JSON.parseObject(value, new TypeReference<HashMap<String, Object>>() {
                                                });

                                        String cookieId = String.valueOf(dataMap.get("cookie_id"));
                                        String userId = String.valueOf(dataMap.get("user_id"));
                                        String eventType = String.valueOf(dataMap.get("event_type"));
                                        String timeStamp = String.valueOf(dataMap.get("unix_time"));

                                        //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                                        Object skuInfo = String.valueOf(dataMap.get("skuinfo"));

                                        List<String> skuInfoList = null;

                                        if (String.valueOf(skuInfo).contains("sku")) {
                                            skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                                        }

                                        if (eventType.equals("order") || eventType.equals("purchase")) {

                                            for (String sku : skuInfoList) {
                                                System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);

                                                out.collect(new Tuple5<>(cookieId, userId, eventType, sku, timeStamp));
                                            }
                                        }
                                    }
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                            }
                        });

        //pc和php数据做合并集合操作
        DataStream<Tuple5<String,String,String,String,String>> resultStream =
                pcResultStream.union(phpResultStream);

        //将数据保存到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        resultStream.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<Tuple5<String,String,String,String,String>>() {
                    public IndexRequest createIndexRequest(Tuple5<String,String,String,String,String> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("cookie_id", element.f0);
                        json.put("user_id", element.f1);
                        json.put("event_type", element.f2);
                        json.put("event_value", element.f3);
//                long timeStamp = Long.valueOf(element.f4.substring(0,element.f4.length()-3));
                        json.put("time_stamp", Long.valueOf(element.f4));

                        return Requests.indexRequest().index("zaful"+"_"+"user"+"_"+element.f2+"_event_realtime")
                                .type("ai-zaful-pc-userinfo").routing(element.f0).source(json);
                    }

                    @Override
                    public void process(Tuple5<String,String,String,String,String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                })).name("zaful_pc_php_kafka_log_to_es").setParallelism(2);

        env.execute("ZafulPcAndPhpUserInfoStreamApp");
    }
}
