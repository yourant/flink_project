package com.globalegrow.tianchi.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.ZafulPcSkuBehavior;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 0:21 2019/11/11
 * @Modified:
 */
public class ZafulPhpSkuFlatMapFunction implements FlatMapFunction<String, ZafulPcSkuBehavior> {

    @Override
    public void flatMap(String value, Collector<ZafulPcSkuBehavior> out) throws Exception {

        try {

            if (StringUtils.isNotBlank(value)) {

                HashMap<String, Object> dataMap =
                        JSON.parseObject(value, new TypeReference<HashMap<String, Object>>() {
                        });

                String cookieId = String.valueOf(dataMap.get("cookie_id"));
                String userId = String.valueOf(dataMap.get("user_id"));
                String eventType = String.valueOf(dataMap.get("event_type"));
                long timeStamp = Long.valueOf(String.valueOf(dataMap.get("unix_time")));

                //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
                Object skuInfo = String.valueOf(dataMap.get("skuinfo"));

//                    System.out.println("skuInfo: "+skuInfo);

                List<String> skuInfoList = null;

                if (String.valueOf(skuInfo).contains("sku")) {
                    skuInfoList = PCFieldsUtils.getSkuFromSkuInfo(skuInfo);
                }

                if (eventType.equals("order") || eventType.equals("purchase")) {

                    for (String sku : skuInfoList) {

                        ZafulPcSkuBehavior behavior = new ZafulPcSkuBehavior(cookieId, userId, eventType, sku, timeStamp);
                        out.collect(behavior);

                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
