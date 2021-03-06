package com.globalegrow.tianchi.transformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.globalegrow.tianchi.bean.PCLogModel;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashMap;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 11:29 2019/11/1
 * @Modified:
 */
public class ZafulPCFilterFunction implements FilterFunction<PCLogModel> {

    @Override
    public boolean filter(PCLogModel value) throws Exception {

        boolean isProcessEvent = false;
        try {

            if (value != null){

                String eventType = value.getEvent_type();

                if (eventType.equals("expose") || eventType.equals("click") ||
                        eventType.equals("adt") || eventType.equals("collect") || eventType.equals("search")) {
                    isProcessEvent = true;
            }
        }
        }catch (Exception e){
            e.printStackTrace();
        }

        return isProcessEvent;
    }
}
