package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.AppBehavior;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 23:28 2019/12/11
 * @Modified:
 */
public class NewAppLogMonitorApp {

    private static final OutputTag<AppBehavior> zfDataTag = new OutputTag<AppBehavior>("zaful") {
    };
    private static final OutputTag<AppBehavior> gbDataTag = new OutputTag<AppBehavior>("gearbest") {
    };

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

        List<String> topics = Arrays.asList(parameterTool.get("zf.topic"),
                parameterTool.get("gb.topic"));

        //初始化kafka自定义DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>(topics, new SimpleStringSchema(), props);
        Boolean startFromEarliest = parameterTool.getBoolean("startFromEarliest", false);
        if (startFromEarliest) {
            flinkKafkaConsumer011.setStartFromEarliest();
        }

        //获取kafka数据
        DataStream<String> streamData = env.addSource(flinkKafkaConsumer011).shuffle();

        SingleOutputStreamOperator<AppBehavior> sideOutputData = streamData
                .process(new ProcessFunction<String, AppBehavior>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<AppBehavior> out) throws Exception {

                        try {

                            Gson gson = new Gson();

                            //解析为对象
                            AppBehavior behavior = gson.fromJson(value, AppBehavior.class);

                            String appId = behavior.getApp_id().toLowerCase();

                            String type = "";

                            if (appId.contains("zaful")){
                                type = "zaful";
                            }else if (appId.contains("gearbest")){
                                type = "gearbest";
                            }

                            switch (type) {
                                case "zaful":
                                    ctx.output(zfDataTag, behavior);
                                    break;
                                case "gearbest":
                                    ctx.output(gbDataTag, behavior);
                                    break;
                                default:
                                    out.collect(behavior);
                                    break;
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                }).uid("new_app_log_process");

        DataStream<AppBehavior> zfData = sideOutputData
                .getSideOutput(zfDataTag);

        DataStream<AppBehavior> gbData = sideOutputData
                .getSideOutput(gbDataTag);

        //将数据保存到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        zfData.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<AppBehavior>() {
                    public IndexRequest createIndexRequest(AppBehavior element) {

                        Map<String, Object> json = new HashMap<>();
                        json.put("data", element);
                        try {

                            json.put("time_stamp", Long.valueOf(element.getUnix_time()));
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        return Requests.indexRequest().index("zaful_app_monitor_event_realtime")
                                .type("ai-zaful-new-app-monitor").source(json);
                    }

                    @Override
                    public void process(AppBehavior element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                })).name("zaful_app_log_sink_es");

        gbData.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<AppBehavior>() {
                    public IndexRequest createIndexRequest(AppBehavior element) {

                        Map<String, Object> json = new HashMap<>();
                        json.put("data", element);
                        try {

                            json.put("time_stamp", Long.valueOf(element.getUnix_time()));
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        return Requests.indexRequest().index("gearbest_app_monitor_event_realtime")
                                .type("ai-gearbest-new-app-monitor").source(json);
                    }

                    @Override
                    public void process(AppBehavior element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                })).name("gearbest_app_log_sink_es");

        env.execute("NewAppLogMonitorApp" + System.currentTimeMillis());
    }
}
