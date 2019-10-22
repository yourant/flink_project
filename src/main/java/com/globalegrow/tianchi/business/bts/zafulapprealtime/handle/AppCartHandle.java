package com.globalegrow.tianchi.business.bts.zafulapprealtime.handle;

import com.globalegrow.tianchi.bean.bts.zafulapprealtime.AppCartCacheValueInfo;
import com.globalegrow.tianchi.bean.bts.zafulapprealtime.AppCartRedisCacheObj;
import com.globalegrow.tianchi.enums.RedisEnums;
import com.globalegrow.tianchi.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;

/**
 * @author zhougenggeng createTime  2019/10/21
 */
@Slf4j
public class AppCartHandle {
    /**
     * 不用考虑4天加购，此类暂时没用
     */

    // 14 天的秒数 86400 * 14
    public static final long DEFAULT_CACHE_SECONDS = 1209600;


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
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<>(kafka_topic, new SimpleStringSchema(), properties);
        DataStream<String> dataStream = see.addSource(flinkKafkaConsumer011).uid(
            "bts_af_add_to_bag_handle_source_" + appNameFilter).shuffle();
        //DataStreamSource<String> dataStreamSource = see.addSource(flinkKafkaConsumer011);
        dataStream.filter(s -> s.contains("/_app.gif?") && s.contains("af_add_to_bag") && s.toLowerCase().contains(appNameFilter)).uid("bts_af_add_to_bag_handle_source_" + appNameFilter ).shuffle().flatMap(new FlatMapFunction<String, AppCartRedisCacheObj>() {
            @Override
            public void flatMap(String value, Collector<AppCartRedisCacheObj> out) throws Exception {

                try {
                    Map appLogMap = AppLogConvertUtil.getAppLogParameters(value);
                    //System.out.println(appLogMap);
                    String eventName = String.valueOf(appLogMap.get("event_name"));
                    String appName = String.valueOf(appLogMap.get("app_name"));
                    //System.out.println(eventName + "    " + appName + "  " + appNameFilter);

                    //System.out.println("af_add_to_bag".equals(eventName) && appName.toLowerCase().contains(appNameFilter));

                    // 判断事件和站点
                    if ("af_add_to_bag".equals(eventName) && appName.toLowerCase().contains(appNameFilter)) {

                        // log.info("zaful app 加购事件 {} ", appLogMap);

                        // System.out.println("zaful app 加购事件埋点 " + appLogMap);

                        // 判断推荐位，bts 三个实验信息是否都不为空，af_sort 是否为 recommend
                        String eventValue = String.valueOf(appLogMap.get("event_value"));
                        Map<String, Object> eventValueMap = JacksonUtil.readValue(eventValue, Map.class);
                        String btsPlatform = String.valueOf(eventValueMap.get("platform"));
                        String btsAfLang = String.valueOf(eventValueMap.get("af_lang"));
                        String btsAfNationalCode = String.valueOf(eventValueMap.get("af_national_code"));
                        String btsAfCountryCode = String.valueOf(eventValueMap.get("af_country_code"));
                        String afInnerMediasource = String.valueOf(eventValueMap.get("af_inner_mediasource"));
                        String afInnerMediasourceAbridge = AppRecommendPosUtil.getAppRecommendPosName(eventValueMap);
                        String btsAfSort = String.valueOf(eventValueMap.get("af_sort"));
                        String appsflyerDeviceId = String.valueOf(eventValueMap.get("appsflyer_device_id"));
                        String afContentId = String.valueOf(eventValueMap.get("af_content_id"));
                        String site = SiteUtil.getAppSite(appName);
                        String afBtsValue = String.valueOf(eventValueMap.get("af_bts"));
                        Map<String, Object> afBtsMap = JacksonUtil.readValue(afBtsValue, Map.class);

                        String btsPlancode = String.valueOf(afBtsMap.get("plancode"));
                        String btsPlanid = String.valueOf(eventValueMap.get("planid"));
                        String btsVersionid = String.valueOf(eventValueMap.get("versionid"));
                        String btsBucketid = String.valueOf(eventValueMap.get("bucketid"));

                            System.out.println(appLogMap);

                            // key 规则 bts_c_cookie_sku
                            String keyPrifix = RedisEnums.bts_c_.name() + appLogMap.get("appsflyer_device_id") + "_";
                            for (String s : afContentId.split(",")) {
                                AppCartCacheValueInfo valueInfo  = new  AppCartCacheValueInfo();
                                valueInfo.setBtsPlatform(btsPlatform);
                                valueInfo.setBtsAfLang(btsAfLang);
                                valueInfo.setBtsAfNationalCode(btsAfNationalCode);
                                valueInfo.setBtsAfCountryCode(btsAfCountryCode);
                                valueInfo.setBtsAfSort(btsAfSort);
                                valueInfo.setBtsPlancode(btsPlancode);
                                out.collect(new AppCartRedisCacheObj().setKey(keyPrifix + s )

                                    .setValueInfo(new AppCartCacheValueInfo().setBtsPlanid(btsPlanid).setBtsVersionid(btsVersionid).setSite(site).setTimestamp((Long) appLogMap.get(NginxLogConvertUtil.TIMESTAMP_KEY))));

                            }



                    }


                } catch (Exception e) {
                    log.error("埋点数据 {} 处理出错", value, e);
                    e.printStackTrace();
                    System.out.println("埋点数据处理出错 "+ e.toString());
                }


            }
        }).uid("bts_af_add_to_bag_handle_source_" + appNameFilter ).returns(AppCartRedisCacheObj.class);
        see.execute(parameterTool.get("job-name", "bts_af_add_to_bag_handle") + System.currentTimeMillis());
    }
}
