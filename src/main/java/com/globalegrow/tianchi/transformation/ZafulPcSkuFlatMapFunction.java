package com.globalegrow.tianchi.transformation;

import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.bean.ZafulPcSkuBehavior;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 0:13 2019/11/11
 * @Modified:
 */
public class ZafulPcSkuFlatMapFunction implements FlatMapFunction<PCLogModel, ZafulPcSkuBehavior> {

    @Override
    public void flatMap(PCLogModel value, Collector<ZafulPcSkuBehavior> out) throws Exception {
        String cookieId = value.getCookie_id();
        String userId = value.getUser_id();
        String eventType = value.getEvent_type();
        long timeStamp = Long.valueOf(value.getUnix_time());

        //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
        String sub_event_field = value.getSub_event_field();
        String skuInfo = value.getSkuinfo();

        int exposeCount = 0;

        if (eventType.equals("expose")){
            exposeCount = 1;
        }else{
            exposeCount = -5;
        }

//        if (eventType.equals("search")){
//            ZafulPcSkuBehavior behavior = new ZafulPcSkuBehavior(cookieId,userId,eventType,value.getSearch_result_word(),timeStamp);
//            out.collect(behavior);
////            out.collect(new Tuple6<>(cookieId,userId,eventType,value.getSearch_result_word(),timeStamp,exposeCount));
//        }

        List<String> eventFiledSkuList = null;

        List<String> skuInfoList = null;

        if (eventType.equals("expose") || eventType.equals("click") ||
                eventType.equals("adt") || eventType.equals("collect")){

            if (sub_event_field.contains("sku")){
                eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);
            }

            if (skuInfo.contains("sku")){
                skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
            }
        }

        if (StringUtils.isNotBlank(sub_event_field) || null!=sub_event_field) {
            eventFiledSkuList = PCFieldsUtils.getSkuFromSubEventFiled(sub_event_field);
        }

        try {

            if (eventFiledSkuList != null || eventFiledSkuList.size() > 0) {

                for (String sku : eventFiledSkuList) {

                    ZafulPcSkuBehavior behavior = new ZafulPcSkuBehavior(cookieId,userId,eventType,sku,timeStamp);
                    out.collect(behavior);

//                    out.collect(new Tuple6<>(cookieId, userId, eventType, sku, timeStamp,exposeCount));

//                                            System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);
                }

            }else {
                if (skuInfoList != null || skuInfoList.size() > 0) {
                    for (String sku : skuInfoList) {

                        ZafulPcSkuBehavior behavior = new ZafulPcSkuBehavior(cookieId,userId,eventType,sku,timeStamp);
                        out.collect(behavior);

//                        out.collect(new Tuple6<>(cookieId, userId, eventType, sku, timeStamp, exposeCount));

//                                            System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);
                    }
                }
            }
        }catch (Exception e){
            System.out.println("数据错误：" + e);
        }
    }
}
