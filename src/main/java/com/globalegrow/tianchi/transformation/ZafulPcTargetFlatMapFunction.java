package com.globalegrow.tianchi.transformation;

import com.globalegrow.tianchi.bean.PCLogModel;
import com.globalegrow.tianchi.bean.bts.zafulapprealtime.PcEventBehahvior;
import com.globalegrow.tianchi.util.DateUtil;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 18:33 2019/11/10
 * @Modified:
 */
public class ZafulPcTargetFlatMapFunction implements FlatMapFunction<PCLogModel, PcEventBehahvior> {

    @Override
    public void flatMap(PCLogModel value, Collector<PcEventBehahvior> out) throws Exception {

        String eventType = value.getEvent_type();
        String timeStamp = value.getUnix_time();

        long time = 0L;

        if (StringUtils.isNotBlank(timeStamp)) {
//            time = DateUtil.timeStamp2DateStr(timeStamp,"yyyyMMddHH");
            time = Long.valueOf(timeStamp);
        }

        String platform = value.getPlatform();
        String countryCode = value.getCountry_code();

        //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
        String sub_event_field = value.getSub_event_field();

        String skuInfo = value.getSkuinfo();

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

        try {

            if (eventFiledSkuList != null || eventFiledSkuList.size() > 0) { //取sub_event_field里的sku

                for (String sku : eventFiledSkuList) {

                    PcEventBehahvior behahvior = new PcEventBehahvior(eventType, sku, time, platform, countryCode);

                    out.collect(behahvior);
//                    out.collect(new Tuple6<>(eventType, sku, time,platform,countryCode,1));

//                  System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);
                }

            }else {
                if (skuInfoList != null || skuInfoList.size() > 0) { //取skuinfo里的sku
                    for (String sku : skuInfoList) {

                        PcEventBehahvior behahvior = new PcEventBehahvior(eventType, sku, time, platform, countryCode);

                        out.collect(behahvior);

//                        out.collect(new Tuple6<>(eventType, sku, time, platform, countryCode, 1));

//                      System.out.println(cookieId + "\t" + userId + "\t" + eventType + "\t" + sku + "\t" + timeStamp);
                    }
                }
            }
        }catch (Exception e){
//          System.out.println("数据错误：" + e);
        }
    }
}
