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
import org.apache.flink.api.java.utils.ParameterTool;
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

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 14:43 2019/11/18
 * @Modified:
 */
public class ZafulPhpUserInfoStreamApp {

    private static Logger logger = LoggerFactory.getLogger(ZafulPCUserInfoStreamApp.class);

    public static void main(String[] args) throws Exception{

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //并行度
        String parallelism = parameterTool.getRequired("parallelism");

        //checkpoit
        String checkpoit = parameterTool.getRequired("checkpoit");

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(Integer.valueOf(parallelism));

        //设置checkpoit
        env.enableCheckpointing(Long.valueOf(checkpoit));

        Properties props = new Properties();

        props.setProperty("bootstrap.servers",parameterTool.getRequired("bootstrap.servers"));
        props.setProperty("group.id", parameterTool.getRequired("group.id"));

        FlinkKafkaConsumer011<String> phpKafkaSource =
                new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), props);
        Boolean startFromEarliest = parameterTool.getBoolean("startFromEarliest", false);
        if (startFromEarliest) {
            phpKafkaSource.setStartFromEarliest();
        }

        //获取kafka数据
        DataStream<String> streamPHPData = env.addSource(phpKafkaSource).shuffle();

        //处理PHP表数据
        DataStream<Tuple5<String,String,String,String,String>> phpResultStream =
                streamPHPData.filter(new PHPFilterFunction()).uid("zaful_php_filter")
                        .flatMap(new FlatMapFunction<String, Tuple5<String,String,String,String,String>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple5<String,String, String, String, String>> out) throws Exception {
                                HashMap<String,Object> dataMap =
                                        JSON.parseObject(value,new TypeReference<HashMap<String,Object>>() {});

                                String cookieId = String.valueOf(dataMap.get("cookie_id"));
                                String userId = String.valueOf(dataMap.get("user_id"));
                                String eventType = String.valueOf(dataMap.get("event_type"));
                                String timeStamp = String.valueOf(dataMap.get("unix_time"));

                                //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                                Object skuInfo =  String.valueOf(dataMap.get("skuinfo"));

                                List<String> skuInfoList = null;

                                if (String.valueOf(skuInfo).contains("sku")){
                                    skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                                }

                                if (eventType.equals("order") || eventType.equals("purchase")){

                                    for (String sku: skuInfoList){

                                        out.collect(new Tuple5<>(cookieId,userId,eventType,sku,timeStamp));
                                    }
                                }
                            }
                        }).uid("php_clean_data_to_es");

        //将数据保存到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        phpResultStream.addSink(new ElasticsearchSink(config, transportAddresses,
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
                }));

        env.execute("ZafulPhpUserInfoStreamApp" + + System.currentTimeMillis());
    }
}
