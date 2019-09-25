package com.globalegrow.tianchi.test.wordcount;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author zhougenggeng createTime  2019/9/25
 */
public class BatchWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "file:///d:/tmp/hello.txt";
        env.setParallelism(1);
        DataStream<String> dataSet = env.readTextFile(inputPath);
        System.out.println(dataSet);
    }
}
