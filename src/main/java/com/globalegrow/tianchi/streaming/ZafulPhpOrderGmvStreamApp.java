package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.PCTargetResultCount;
import com.globalegrow.tianchi.bean.PhpOrderBehavior;
import com.globalegrow.tianchi.bean.PhpOrderSum;
import com.globalegrow.tianchi.bean.bts.zafulapprealtime.PcEventBehahvior;
import com.globalegrow.tianchi.transformation.PHPFilterFunction;
import com.globalegrow.tianchi.transformation.PhpOrderFlatMapFunction;
import com.globalegrow.tianchi.transformation.PhpTargetFlatMapFunction;
import com.globalegrow.tianchi.util.DateUtil;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
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
 * @Date: Created in 20:22 2019/11/10
 * @Modified:
 */
public class ZafulPhpOrderGmvStreamApp {

    public static void main(String[] args) throws Exception{

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置EventTime为事件事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置全局并发数
        env.setParallelism(1);

        //获取kafka DataSource
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "172.31.35.194:9092,172.31.50.250:9092,172.31.63.112:9092");
        props.setProperty("group.id", "zaful_php_order");

        FlinkKafkaConsumer011<String> phpKafkaSource =
                new FlinkKafkaConsumer011<>("glbg-analitic-zaful-php", new SimpleStringSchema(), props);

        //获取kafka数据
        DataStreamSource<String> streamPHPData = env.addSource(phpKafkaSource);

        //处理php数据
        DataStream<PhpOrderSum> phpResultStream = streamPHPData
                .filter(new PHPFilterFunction())
                .flatMap(new PhpOrderFlatMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PhpOrderBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(PhpOrderBehavior element) {
                        return element.getTimeStamp() * 1000;
                    }
                })
                .map(new MapFunction<PhpOrderBehavior, PhpOrderSum>() {
                    @Override
                    public PhpOrderSum map(PhpOrderBehavior value) throws Exception {
                        return new PhpOrderSum(value.getEventType(), value.getSku(), value.getPrice()*value.getPam(),
                        String.valueOf(value.getTimeStamp()), value.getPlatform());
                    }
                })
                .keyBy("eventType","sku","platform")
                .timeWindow(Time.minutes(60))
                .allowedLateness(Time.seconds(10))
                .reduce(new SumGmv(), new WindowRumResultFunction());

        //将统计结果sink到ES
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "esearch-aws-dy");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.47.84"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.43.158"), 9302));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.31.55.231"), 9302));

        phpResultStream.addSink(new ElasticsearchSink(config, transportAddresses,
                new ElasticsearchSinkFunction<PhpOrderSum>() {
                    public IndexRequest createIndexRequest(PhpOrderSum element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("event_type", element.getEventType());
                        json.put("event_value", element.getSku());
                        json.put("time_stamp", element.getTimeStamp());
                        json.put("platform", element.getPlatform());
                        json.put("country", "");
                        json.put("target_value", String.valueOf(element.getAmount()));

                        return Requests.indexRequest().index("zaful_target_event_realtime_pro")
                                .type("ai-zaful-pc-target").source(json);
                    }

                    @Override
                    public void process(PhpOrderSum element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }));

        env.execute("ZafulPhpOrderGmvStreamApp");
    }

    public static class WindowRumResultFunction implements WindowFunction<PhpOrderSum, PhpOrderSum, Tuple, TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<PhpOrderSum> input, Collector<PhpOrderSum> out) throws Exception {

            String eventType = ((Tuple3<String,String,String>) tuple).f0;

            String sku = ((Tuple3<String,String,String>) tuple).f1;

            String timeStamp = DateUtil.timeStamp2DateStr(String.valueOf(window.getEnd()), "yyyyMMddHH");

            String platform = ((Tuple3<String,String,String>) tuple).f2;

            Double gmv = input.iterator().next().getAmount();

            out.collect(new PhpOrderSum(eventType,sku,gmv,timeStamp,platform));
        }
    }

    public static class SumGmv implements ReduceFunction<PhpOrderSum>{
        @Override
        public PhpOrderSum reduce(PhpOrderSum value1, PhpOrderSum value2) throws Exception {
            return new PhpOrderSum(value1.getEventType(),value1.getSku(),value1.getAmount()+value2.getAmount(),
                    value1.getTimeStamp(),value1.getPlatform());
        }
    }

}
