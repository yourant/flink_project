package com.globalegrow.tianchi.transformation;

import com.globalegrow.tianchi.bean.PCLogModel;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 11:29 2019/11/1
 * @Modified:
 */
public class ZafulPCEventFilterFunction implements FilterFunction<PCLogModel> {

    @Override
    public boolean filter(PCLogModel value) throws Exception {
        boolean isProcessEvent = false;
        if (null != value) {

            String eventType = value.getEvent_type();


            if (eventType.equals("expose") || eventType.equals("click") ||
                    eventType.equals("adt") || eventType.equals("collect")) {
                isProcessEvent = true;
            }

        }
        return isProcessEvent;
    }
}
