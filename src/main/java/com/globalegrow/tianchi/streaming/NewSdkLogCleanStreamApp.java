package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.AppBehavior;
import com.globalegrow.tianchi.transformation.AppObject2JsonFlatMapFunction;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 10:29 2019/12/3
 * @Modified:
 */
public class NewSdkLogCleanStreamApp {

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

        props.setProperty("bootstrap.servers",parameterTool.getRequired("source.bootstrap.servers"));
        props.setProperty("group.id", parameterTool.getRequired("group.id"));

        //初始化kafka自定义DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), props);
        Boolean startFromEarliest = parameterTool.getBoolean("startFromEarliest", false);
        if (startFromEarliest) {
            flinkKafkaConsumer011.setStartFromEarliest();
        }

        //获取kafka数据
        DataStream<String> streamData = env.addSource(flinkKafkaConsumer011)
//                .setParallelism(Integer.valueOf(parameterTool.getRequired("source.parallelism")))
                .shuffle();

        SingleOutputStreamOperator<AppBehavior> sideOutputData = streamData
            .flatMap(new FlatMapFunction<String, AppBehavior>() {
                @Override
                public void flatMap(String value, Collector<AppBehavior> out) throws Exception {

                    try {

                        //反转义字符串
                        String jsonStr = StringEscapeUtils.unescapeJava(value);

                        String unixTime = jsonStr.substring(0, 10);

                        Gson gson = new Gson();

                        //解析为对象
                        AppBehavior behavior = gson.fromJson(
                                jsonStr.substring(10, jsonStr.length()), AppBehavior.class);

                        behavior.setUnix_time(unixTime);

                        out.collect(behavior);

                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }).uid("app_log_clean_json_to_behavior_flatmap")
            .process(new ProcessFunction<AppBehavior, AppBehavior>() {
                @Override
                public void processElement(AppBehavior value, Context ctx, Collector<AppBehavior> out) throws Exception {

                    String appId = value.getApp_id().toLowerCase();

                    String type = "";

                    if (appId.contains("zaful")){
                        type = "zaful";
                    }else if (appId.contains("gearbest")){
                        type = "gearbest";
                    }

                    switch (type) {
                        case "zaful":
                            ctx.output(zfDataTag, value);
                            break;
                        case "gearbest":
                            ctx.output(gbDataTag, value);
                            break;
                        default:
                            out.collect(value);
                            break;
                    }
                }
            }).uid("app_log_clean_side_out_process");
//                .setParallelism(Integer.valueOf(parameterTool.getRequired("process.parallelism")));

        DataStream<String> zfData = sideOutputData
                .getSideOutput(zfDataTag)
                .flatMap(new AppObject2JsonFlatMapFunction())
                .uid("app_log_clean_zfDataTag_flatmap");

        DataStream<String> gbData = sideOutputData
                .getSideOutput(gbDataTag)
                .flatMap(new AppObject2JsonFlatMapFunction())
                .uid("app_log_clean_gbDataTag_flatmap");

        FlinkKafkaProducer011<String> zfProducer = new FlinkKafkaProducer011<>(
                parameterTool.getRequired("sink.bootstrap.servers"),
                parameterTool.getRequired("zf.topic"),
                new SimpleStringSchema());

        zfData.addSink(zfProducer)
                .name("app_log_clean_zfData_sink")
                .uid("app_log_clean_zfDataTag_sink_kafka");

        FlinkKafkaProducer011<String> gbProducer = new FlinkKafkaProducer011<>(
                parameterTool.getRequired("sink.bootstrap.servers"),
                parameterTool.getRequired("gb.topic"),
                new SimpleStringSchema());

        gbData.addSink(gbProducer)
                .name("app_log_clean_gbData_sink")
                .uid("app_log_clean_gbDataTag_sink_kafka");

        env.execute("NewSdkLogCleanStreamApp" + System.currentTimeMillis());
    }
}
