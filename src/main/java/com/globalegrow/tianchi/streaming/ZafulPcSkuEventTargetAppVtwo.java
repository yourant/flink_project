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

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置EventTime为事件事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置全局并发数
        env.setParallelism(5);

        //获取kafka DataSource
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "172.31.35.194:9092,172.31.50.250:9092,172.31.63.112:9092");
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

        OutputTag<PcEventBehahvior> lateDataTag = new OutputTag<PcEventBehahvior>("late"){};

        //统计各个指标
        SingleOutputStreamOperator<PCTargetResultCount> resultStream = targetCount(streamPCData, streamPHPData,lateDataTag);

        //存放延迟数据
        DataStream<PcEventBehahvior> lateData = resultStream.getSideOutput(lateDataTag);

        //将统计结果sink到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

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
                }));

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
                }));

        env.execute("ZafulPcSkuEventTargetAppVtwo");
    }

    /**
     * 计数统计
     * @param streamPCData
     * @param streamPHPData
     * @return
     */
    public static SingleOutputStreamOperator<PCTargetResultCount> targetCount(DataStreamSource<String> streamPCData,
                                                                              DataStreamSource<String> streamPHPData,
                                                                              OutputTag<PcEventBehahvior> lateDataTag){
        //处理zaful pc端数据
        DataStream<PcEventBehahvior> pcResultStream = streamPCData
                .map(new MapFunction<String, PCLogModel>() { //解析kafka json字符串为PCLogModel对象
                    @Override
                    public PCLogModel map(String value) throws Exception {

                        PCLogModel pcLogModel = PCFieldsUtils.getPCLogModel(value);

                        return pcLogModel;
                    }
                })
                .filter(new ZafulPCFilterFunction()) //过滤掉不需要的数据
                .flatMap(new ZafulPcTargetFlatMapFunction()); //取需要的事件数据

        //处理php数据
        DataStream<PcEventBehahvior> phpResultStream = streamPHPData
                .filter(new PHPFilterFunction())
                .flatMap(new PhpTargetFlatMapFunction());

        //合并Stream并进行聚合操作
        SingleOutputStreamOperator<PCTargetResultCount> resultStream = pcResultStream.union(phpResultStream)
                //抽取时间和生成watermark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PcEventBehahvior>() {
                    @Override
                    public long extractAscendingTimestamp(PcEventBehahvior element) {
                        return element.getTimeStamp() * 1000;
                    }
                })
                .keyBy("eventType","sku","platform","countryCode")
                .timeWindow(Time.minutes(60))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(lateDataTag)
                .aggregate(new CountAgg(), new WindowResultFunction());

        return resultStream;
    }

    /**
     * 用于输出窗口函数
     */
    public static class WindowResultFunction implements WindowFunction<Long, PCTargetResultCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, //窗口主键
                          TimeWindow window, //窗口
                          Iterable<Long> input, //聚合函数的结果，即count值
                          Collector<PCTargetResultCount> out //输出类型为 PCTargetResultCount
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
     * COUNT 统计到聚合函数的实现，每出现一条记录加1
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
