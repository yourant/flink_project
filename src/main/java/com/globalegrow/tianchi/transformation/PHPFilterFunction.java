package com.globalegrow.tianchi.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.util.PCFieldsUtils;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashMap;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 11:42 2019/11/1
 * @Modified:
 */
public class PHPFilterFunction implements FilterFunction<String> {

    @Override
    public boolean filter(String value) throws Exception {
        HashMap<String,Object> dataMap =
                JSON.parseObject(value,new TypeReference<HashMap<String,Object>>() {});

        String eventType = (String)dataMap.get("event_type");

        String platform = (String)dataMap.get("platform");

        boolean isZafulEvent = false;

        if (PCFieldsUtils.isZafulCodeSite(value) && (eventType.equals("order") || eventType.equals("purchase"))
         && platform.equals("pc")){
            isZafulEvent = true;
        }

        return isZafulEvent;
    }
}
