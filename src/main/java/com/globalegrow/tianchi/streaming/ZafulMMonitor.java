package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 19:33 2019/11/22
 * @Modified:
 */
public class ZafulMMonitor {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

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

        props.setProperty("bootstrap.servers",parameterTool.getRequired("bootstrap.servers"));
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

        DataStream<Tuple2<PCLogModel, String>> result = streamPCData
                .map(new MapFunction<String, Tuple2<PCLogModel,String>>() {
                    @Override
                    public Tuple2<PCLogModel,String> map(String value) throws Exception {

                        PCLogModel pcLogModel = null;

                        try {

                            pcLogModel = PCFieldsUtils.getPCLogModel(value);

                        }catch (Exception e){
                            e.printStackTrace();
                        }

                        return new Tuple2<>(pcLogModel, pcLogModel.getUnix_time());
                    }
                }).uid("zaful_m_map")
                .filter(new FilterFunction<Tuple2<PCLogModel, String>>() {
                    @Override
                    public boolean filter(Tuple2<PCLogModel, String> value) throws Exception {

                        boolean isProcessEvent = false;
                        try {

                            if (value.f0.getEvent_type().equals("adt")){

                                String eventType = value.f0.getEvent_type();

                                isProcessEvent = true;
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        return isProcessEvent;
                    }
                }).uid("zaful_m_filter");

        //将数据保存到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        result.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<Tuple2<PCLogModel, String>>() {
                    public IndexRequest createIndexRequest(Tuple2<PCLogModel, String> element) {

                        Map<String, Object> json = new HashMap<>();
                        json.put("data", element.f0);
                        try {

                            json.put("time_stamp", Long.valueOf(element.f1));
                        }catch (Exception e){
                            e.printStackTrace();
                        }
//
                        return Requests.indexRequest().index("zaful_m_monitor_event_realtime")
                                .type("ai-zaful-m-monitor").source(json);
                    }

                    @Override
                    public void process(Tuple2<PCLogModel, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                })).name("zaful_m_userInfo_sink_es");

        env.execute("ZafulMMonitor" + System.currentTimeMillis());
    }
}
