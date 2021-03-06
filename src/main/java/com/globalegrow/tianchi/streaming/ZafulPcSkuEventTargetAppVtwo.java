package com.globalegrow.tianchi.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.AmountModel;
import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.bean.PCTargetResultCount;
import com.globalegrow.tianchi.bean.bts.zafulapprealtime.PcEventBehahvior;
import com.globalegrow.tianchi.transformation.PHPFilterFunction;
import com.globalegrow.tianchi.transformation.PhpTargetFlatMapFunction;
import com.globalegrow.tianchi.transformation.ZafulPCFilterFunction;
import com.globalegrow.tianchi.transformation.ZafulPcTargetFlatMapFunction;
import com.globalegrow.tianchi.util.DateUtil;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.expressions.E;
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
 * @Date: Created in 18:21 2019/11/10
 * @Modified:
 */
public class ZafulPcSkuEventTargetAppVtwo {

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

        //??????EventTime???????????????
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //??????kafka DataSource
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        props.setProperty("group.id", parameterTool.getRequired("group.id"));

        //?????????kafka?????????DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>(parameterTool.getRequired("pc.topic"), new SimpleStringSchema(), props);
        Boolean startFromEarliest = parameterTool.getBoolean("startFromEarliest", false);
        if (startFromEarliest) {
            flinkKafkaConsumer011.setStartFromEarliest();
        }

        FlinkKafkaConsumer011<String> phpKafkaSource =
                new FlinkKafkaConsumer011<>(parameterTool.getRequired("php.topic"), new SimpleStringSchema(), props);
        if (startFromEarliest) {
            phpKafkaSource.setStartFromEarliest();
        }

        //??????kafka??????
        DataStream<String> streamPCData = env.addSource(flinkKafkaConsumer011).shuffle();

        //??????kafka??????
        DataStream<String> streamPHPData = env.addSource(phpKafkaSource).shuffle();

        OutputTag<PcEventBehahvior> lateDataTag = new OutputTag<PcEventBehahvior>("late"){};

        //??????????????????
        SingleOutputStreamOperator<PCTargetResultCount> resultStream = targetCount(streamPCData, streamPHPData,lateDataTag);

        //??????????????????
        DataStream<PcEventBehahvior> lateData = resultStream.getSideOutput(lateDataTag);

        //???????????????sink???ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        resultStream.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<PCTargetResultCount>() {
                    public IndexRequest createIndexRequest(PCTargetResultCount element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("event_type", element.getEventType());
                        json.put("event_value", element.getSku());
                        json.put("time_stamp", element.getTimeStamp());
                        json.put("platform", element.getPlatform());
                        json.put("country", element.getCountryCode().toUpperCase());
                        json.put("target_value", String.valueOf(element.getViewCount()));

                        return Requests.indexRequest().index("zaful_target_event_realtime_pro")
                                .type("ai-zaful-pc-target").source(json);
                    }

                    @Override
                    public void process(PCTargetResultCount element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                })).name("zaful_pcm_target_sink_es");

        lateData.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<PcEventBehahvior>() {
                    public IndexRequest createIndexRequest(PcEventBehahvior element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("event_type", element.getEventType());
                        json.put("event_value", element.getSku());
                        json.put("time_stamp", element.getTimeStamp());
                        json.put("platform", element.getPlatform());
                        json.put("country", element.getCountryCode().toUpperCase());

                        return Requests.indexRequest().index("zaful_target_late_event_realtime")
                                .type("ai-zaful-pc-target").source(json);
                    }

                    @Override
                    public void process(PcEventBehahvior element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                })).name("zaful_pcm_target_late_data_sink_es");

        env.execute("ZafulPcSkuEventTargetAppVtwo" + System.currentTimeMillis());
    }

    /**
     * ????????????
     * @param streamPCData
     * @param streamPHPData
     * @return
     */
    public static SingleOutputStreamOperator<PCTargetResultCount> targetCount(DataStream<String> streamPCData,
                                                                              DataStream<String> streamPHPData,
                                                                              OutputTag<PcEventBehahvior> lateDataTag){
        //??????zaful pc?????????
        DataStream<PcEventBehahvior> pcResultStream = streamPCData
                .map(new MapFunction<String, PCLogModel>() { //??????kafka json????????????PCLogModel??????
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
                }).uid("zaful_pc_map")
                .filter(new ZafulPCFilterFunction()).uid("zaful_pc_filter") //???????????????????????????
                .flatMap(new ZafulPcTargetFlatMapFunction()).uid("zaful_pc_flatmap"); //????????????????????????

        //??????php??????
        DataStream<PcEventBehahvior> phpResultStream = streamPHPData
                .filter(new PHPFilterFunction()).uid("zaful_php_filter")
                .flatMap(new PhpTargetFlatMapFunction()).uid("zaful_hph_flatmap");

        //??????Stream?????????????????????
        SingleOutputStreamOperator<PCTargetResultCount> resultStream = pcResultStream.union(phpResultStream)
                //?????????????????????watermark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PcEventBehahvior>() {
                    @Override
                    public long extractAscendingTimestamp(PcEventBehahvior element) {
                        return element.getTimeStamp() * 1000;
                    }
                }).uid("target_zaful_pcm_window")
                .keyBy("eventType","sku","platform","countryCode")
                .timeWindow(Time.minutes(60))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(lateDataTag)
                .aggregate(new CountAgg(), new WindowResultFunction()).uid("target_zaful_pcm_aggregate");

        return resultStream;
    }

    /**
     * ????????????????????????
     */
    public static class WindowResultFunction implements WindowFunction<Long, PCTargetResultCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, //????????????
                          TimeWindow window, //??????
                          Iterable<Long> input, //???????????????????????????count???
                          Collector<PCTargetResultCount> out //??????????????? PCTargetResultCount
        ) throws Exception {

            String eventType = ((Tuple4<String,String,String,String>) tuple).f0;

            String sku = ((Tuple4<String,String,String,String>) tuple).f1;

            String timeStamp = DateUtil.timeStamp2DateStr(String.valueOf(window.getEnd()), "yyyyMMddHH");

            String platform = ((Tuple4<String,String,String,String>) tuple).f2;

            String countryCode = ((Tuple4<String,String,String,String>) tuple).f3;

            long viewCount = input.iterator().next();

            out.collect(new PCTargetResultCount(eventType,sku,timeStamp,platform,countryCode,viewCount));

        }
    }

    /**
     * COUNT ?????????????????????????????????????????????????????????1
     */
    public static class CountAgg implements AggregateFunction<PcEventBehahvior, Long, Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(PcEventBehahvior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
