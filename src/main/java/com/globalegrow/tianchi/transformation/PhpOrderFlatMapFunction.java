package com.globalegrow.tianchi.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.AmountModel;
import com.globalegrow.tianchi.bean.PhpOrderBehavior;
import com.globalegrow.tianchi.util.DateUtil;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 20:33 2019/11/10
 * @Modified:
 */
public class PhpOrderFlatMapFunction implements FlatMapFunction<String, PhpOrderBehavior> {

    @Override
    public void flatMap(String value, Collector<PhpOrderBehavior> out) throws Exception {
        HashMap<String,Object> dataMap =
                JSON.parseObject(value,new TypeReference<HashMap<String,Object>>() {});

        String cookieId = String.valueOf(dataMap.get("cookie_id"));
        String userId = String.valueOf(dataMap.get("user_id"));
        String eventType = String.valueOf(dataMap.get("event_type"));
        String timeStamp = String.valueOf(dataMap.get("unix_time"));

        long time = 0L;

        if (StringUtils.isNotBlank(timeStamp)){
            time = Long.valueOf(timeStamp);
        }

        String platform = String.valueOf(dataMap.get("platform"));
        String country_number = String.valueOf(dataMap.get("country_number"));

        //取skuinfo和sub_event_field的sku值，有可能是数组json格式，也有可能直接是json格式
        Object skuInfo =  String.valueOf(dataMap.get("skuinfo"));

        List<AmountModel> skuInfoList = null;

        if (String.valueOf(skuInfo).contains("sku")){
            skuInfoList = PCFieldsUtils.getSkuAmountFromSkuInfo(skuInfo);
        }

        if (eventType.equals("order") || eventType.equals("purchase")){

            for (AmountModel sku: skuInfoList){
//              System.out.println(cookieId + "\t"+userId+"\t" + eventType + "\t" + sku + "\t" + timeStamp);

                String orderGmv = "";

                if (eventType.equals("order")){
                    orderGmv = "gmv";
                }else {
                    orderGmv = "sales_value";
                }

                PhpOrderBehavior behavior = new PhpOrderBehavior(cookieId, userId, orderGmv, sku.getSku(), sku.getPrice(), sku.getPam(),time,platform,"");

                out.collect(behavior);
//                out.collect(new Tuple9<>(cookieId,userId,eventType,sku.getSku(),sku.getPrice(),sku.getPam(),time,platform,country_number));
            }
        }
    }
}
