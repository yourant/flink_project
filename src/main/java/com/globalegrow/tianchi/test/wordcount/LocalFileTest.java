package com.globalegrow.tianchi.test.wordcount;

import com.globalegrow.tianchi.bean.AppBehavior;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: xhuan_wang
 * @Description:
 * @Date: Created in 10:30 2019/12/3
 * @Modified:
 */
public class LocalFileTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.readTextFile("file:///E:/tmp/flink/input/app.txt");

//        data.flatMap(new FlatMapFunction<String, AppBehavior>() {
//            @Override
//            public void flatMap(String value, Collector<AppBehavior> out) throws Exception {
//
//                try {
//
//                    //反转义字符串
//                    String jsonStr = StringEscapeUtils.unescapeJava(value);
//
//                    Gson gson = new Gson();
//
//                    //解析为对象
//                    AppBehavior behavior = gson.fromJson(
//                            jsonStr.substring(jsonStr.indexOf("{"), jsonStr.length()), AppBehavior.class);
//
//                    out.collect(behavior);
//
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
//            }
//        });

        env.execute("LocalFileTest");
    }
}
