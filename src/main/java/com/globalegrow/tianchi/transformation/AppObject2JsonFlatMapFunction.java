package com.globalegrow.tianchi.transformation;

import com.globalegrow.tianchi.bean.AppBehavior;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 20:01 2019/12/4
 * @Modified:
 */
public class AppObject2JsonFlatMapFunction implements FlatMapFunction<AppBehavior, String> {

    @Override
    public void flatMap(AppBehavior value, Collector<String> out) throws Exception {
        Gson gson = new Gson();
        String jsonStr = gson.toJson(value);
        out.collect(jsonStr);
    }
}
