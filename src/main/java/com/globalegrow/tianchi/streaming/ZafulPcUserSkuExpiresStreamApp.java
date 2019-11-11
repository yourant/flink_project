package com.globalegrow.tianchi.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.bean.PCTargetResultCount;
import com.globalegrow.tianchi.bean.ZafulPcFeedBackCount;
import com.globalegrow.tianchi.bean.ZafulPcSkuBehavior;
import com.globalegrow.tianchi.bean.bts.zafulapprealtime.PcEventBehahvior;
import com.globalegrow.tianchi.transformation.PHPFilterFunction;
import com.globalegrow.tianchi.transformation.ZafulPCFilterFunction;
import com.globalegrow.tianchi.transformation.ZafulPcSkuFlatMapFunction;
import com.globalegrow.tianchi.transformation.ZafulPhpSkuFlatMapFunction;
import com.globalegrow.tianchi.util.DateUtil;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 0:07 2019/11/11
 * @Modified:
 */
public class ZafulPcUserSkuExpiresStreamApp {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(3);

        Properties props = new Properties();

        props.setProperty("bootstrap.servers","172.31.35.194:9092,172.31.50.250:9092,172.31.63.112:9092");
        props.setProperty("group.id", "zaful_pc_userinfo");

        //初始化kafka自定义DataSource
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 =
                new FlinkKafkaConsumer011<>("glbg-analitic-zaful-pc", new SimpleStringSchema(), props);

        //获取kafka数据
        DataStreamSource<String> streamPCData = env.addSource(flinkKafkaConsumer011);

//        DataStreamSource<String> streamPCData = env.readTextFile("file:///E:\\tmp\\flink\\input\\pc_log.txt");

        DataStream<ZafulPcFeedBackCount> pcResultStream = streamPCData
                .map(new MapFunction<String, PCLogModel>() {
                    @Override
                    public PCLogModel map(String value) throws Exception {

                        PCLogModel pcLogModel = PCFieldsUtils.getPCLogModel(value);

                        return pcLogModel;
                    }
                })
                .filter(new FilterFunction<PCLogModel>() {
                    @Override
                    public boolean filter(PCLogModel value) throws Exception {
                        String eventType = value.getEvent_type();

                        boolean isProcessEvent = false;

                        if (eventType.equals("expose")){
                            isProcessEvent = true;
                        }

                        return isProcessEvent;
                    }
                })
                .flatMap(new ZafulPcSkuFlatMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ZafulPcSkuBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(ZafulPcSkuBehavior element) {
                        return element.getTimeStamp();
                    }
                })
                .keyBy("cookieId","sku")
                .timeWindow(Time.minutes(30))
                .aggregate(new CountAgg(), new WindowResultFunction());

        pcResultStream.print();

        //将统计结果sink到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        pcResultStream.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<ZafulPcFeedBackCount>() {
                    public IndexRequest createIndexRequest(ZafulPcFeedBackCount element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("cookie_id", element.getCookieId());
                        json.put("sku", element.getSku());
                        json.put("time_stamp", element.getTimeStamp());
                        json.put("expose_count", element.getFeedBack());

                        return Requests.indexRequest().index("zaful_pc_expose_count_realtime")
                                .type("ai-zaful-pc-expose-count").routing(element.getCookieId()).source(json);
                    }

                    @Override
                    public void process(ZafulPcFeedBackCount element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }));

        env.execute("ZafulPcUserSkuExpiresStreamApp");
    }


    /**
     * 用于输出窗口函数
     */
    public static class WindowResultFunction implements WindowFunction<Long, ZafulPcFeedBackCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, //窗口主键
                          TimeWindow window, //窗口
                          Iterable<Long> input, //聚合函数的结果，即count值
                          Collector<ZafulPcFeedBackCount> out //输出类型为 PCTargetResultCount
        ) throws Exception {

            String eventType = ((Tuple2<String,String>) tuple).f0;

            String sku = ((Tuple2<String,String>) tuple).f1;

//            String timeStamp = DateUtil.timeStamp2DateStr(String.valueOf(window.getEnd()*1000), "yyyyMMddHH");

            long viewCount = input.iterator().next();

            out.collect(new ZafulPcFeedBackCount(eventType, sku, window.getEnd()*1000, viewCount));

        }
    }

    /**
     * COUNT 统计到聚合函数的实现，每出现一条记录加1
     */
    public static class CountAgg implements AggregateFunction<ZafulPcSkuBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ZafulPcSkuBehavior value, Long accumulator) {

            long exposeCount = 0L;

            if (value.getEventType().equals("expose")){
                exposeCount = accumulator + 1;
            }

            return exposeCount;
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
