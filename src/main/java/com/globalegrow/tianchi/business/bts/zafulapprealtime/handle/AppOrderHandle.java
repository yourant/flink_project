package com.globalegrow.tianchi.business.bts.zafulapprealtime.handle;

import com.globalegrow.tianchi.bean.bts.zafulapprealtime.AppOrderModel;
import com.globalegrow.tianchi.business.bts.sink.es.ElasticSearchOutputFormat;
import com.globalegrow.tianchi.business.bts.sink.es.ElasticsearchSinkFunction;
import com.globalegrow.tianchi.business.bts.sink.es.RequestIndexer;
import com.globalegrow.tianchi.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @author zhougenggeng createTime  2019/10/22
 */
@Slf4j
public class AppOrderHandle {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.enableCheckpointing(5000);
        see.setParallelism(parameterTool.getInt("job.parallelism", 1));
        //see.getConfig().setGlobalJobParameters(parameterTool);
        Properties pro = PropertiesUtil.loadProperties("config.properties");
        String kafka_topic = pro.getProperty("bts.kafka.topic");
        String bootstrap_servers = pro.getProperty("bts.kafka.broker.list");
        String kafka_group = pro.getProperty("bts.kafka.group");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap_servers);
        properties.setProperty("group.id", kafka_group);

        String appNameFilter = parameterTool.getRequired("log.app.filter");
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<>(kafka_topic,
            new SimpleStringSchema(), properties);
        List<String> listEventName = new ArrayList();
        listEventName.add("af_impression");
        listEventName.add("af_view_product");
        listEventName.add("af_add_to_bag");
        listEventName.add("af_add_to_wishlis");
        listEventName.add("af_search");
        listEventName.add("af_create_order_success");
        listEventName.add("af_purchase");
        SingleOutputStreamOperator<AppOrderModel> returns = see.addSource(flinkKafkaConsumer011).uid(
            "app_order_event_handle_source_" + appNameFilter).shuffle()
            .filter(s -> listEventName.contains(s)).flatMap(
                new FlatMapFunction<String, AppOrderModel>() {
                    @Override
                    public void flatMap(String value, Collector<AppOrderModel> out) throws Exception {
                        try {
                            Map appLogMap = AppLogConvertUtil.getAppLogParameters(value);
                            String eventName = String.valueOf(appLogMap.get("event_name"));
                            String appName = String.valueOf(appLogMap.get("app_name"));
                            // 判断事件和站点
                            if (listEventName.contains(eventName) && appName.toLowerCase().contains(appNameFilter)) {
                                String eventValue = String.valueOf(appLogMap.get("event_value"));
                                Map<String, Object> eventValueMap = JacksonUtil.readValue(eventValue, Map.class);
                                String btsPlatform = String.valueOf(eventValueMap.get("platform"));
                                String btsAfLang = String.valueOf(eventValueMap.get("af_lang"));
                                String btsAfNationalCode = String.valueOf(eventValueMap.get("af_national_code"));
                                String btsAfCountryCode = String.valueOf(eventValueMap.get("af_country_code"));
                                String afInnerMediasource = String.valueOf(eventValueMap.get("af_inner_mediasource"));
                                String afInnerMediasourceAbridge = AppRecommendPosUtil.getAppRecommendPosName(
                                    eventValueMap);
                                String btsAfSort = String.valueOf(eventValueMap.get("af_sort"));
                                String appsflyerDeviceId = String.valueOf(eventValueMap.get("appsflyer_device_id"));
                                String afContentId = String.valueOf(eventValueMap.get("af_content_id"));
                                String afChangedSizeOrColor = String.valueOf(
                                    eventValueMap.get("af_changed_size_or_color"));
                                String afQuantity = String.valueOf(eventValueMap.get("af_quantity"));
                                String afPrice = String.valueOf(eventValueMap.get("af_price"));
                                String site = SiteUtil.getAppSite(appName);
                                String afBtsValue = String.valueOf(eventValueMap.get("af_bts"));
                                Map<String, Object> afBtsMap = JacksonUtil.readValue(afBtsValue, Map.class);

                                String btsPlancode = String.valueOf(afBtsMap.get("plancode"));
                                String btsPlanid = String.valueOf(eventValueMap.get("planid"));
                                String btsVersionid = String.valueOf(eventValueMap.get("versionid"));
                                String btsBucketid = String.valueOf(eventValueMap.get("bucketid"));

                                System.out.println(appLogMap);
                                AppOrderModel appOrderModel = new AppOrderModel();
                                //todo
                                out.collect(appOrderModel);
                                //                                }

                            }

                        } catch (Exception e) {
                            log.error("处理埋点数据 {} 出错", value, e);
                        }

                    }
                }).uid("dy_app_order_event_handle_flatmap_" + appNameFilter).returns(AppOrderModel.class);
        Map<String, String> configEs = new HashMap<>();
        //configEs.put("bulk.flush.max.actions", parameterTool.getRequired("bulk.flush.max.actions"));   // flush inserts after every event
        configEs.put("cluster.name", parameterTool.get("cluster.name", "esearch-aws-dy")); // default cluster name

        List<InetSocketAddress> transports = new ArrayList<>();
        // set default connection details
        String elkServers = parameterTool.get("elastic-servers", "172.31.47.84:9302,172.31.43.158:9302,172.31.55.231:9302");
        for (String s : elkServers.split(",")) {
            transports.add(new InetSocketAddress(InetAddress.getByName(s.split(":")[0]), Integer.valueOf(s.split(":")[1])));
        }

        //sinkEs(configEs, transports, parameterTool.get("index-name", "dy_zaful_goods_info"), parameterTool.get("type-name", "goods_info"), parameterTool.get("route-field", "item_id"), returns);


        //Table table = tableEnv.sqlQuery(parameterTool.getRequired("query.sql"));

        see.execute(parameterTool.get("job-name", "zaful-goods-base-info-es") + "_" + System.currentTimeMillis());
    }
    //public static void sinkEs(Map<String, String> config, List<InetSocketAddress> transportAddresses, String indexName, String typeName, String routeField, DataSet<AppOrderModel> monthStatisticsEs) {
    //    monthStatisticsEs.output(new ElasticSearchOutputFormat<AppOrderModel>(config, transportAddresses, new ElasticsearchSinkFunction<AppOrderModel>() {
    //        @Override
    //        public void process(Map appDataModel, RuntimeContext ctx, RequestIndexer requestIndexer) {
    //            String device_id = String.valueOf(appDataModel.get(routeField));
    //            requestIndexer.add(
    //                Requests.indexRequest().type(typeName).index(indexName).id(device_id).routing(device_id).source(appDataModel));
    //        }
    //
    //    }));
    //}
}
