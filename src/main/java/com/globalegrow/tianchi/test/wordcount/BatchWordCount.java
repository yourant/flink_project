package com.globalegrow.tianchi.test.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.util.Collector;

/**
 * @author zhougenggeng createTime  2019/9/25
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //String inputPath = args[0];
        String inputPath = "d:/tmp/hello.txt";
        //String outPath = args[1];
        String outPath = "d:/tmp/helloout.txt";

        DataSource<String> text = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> counts =
            // split up the lines in pairs (2-tuples) containing: (word,1)
            text.flatMap(new Tokenizer())
                // group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0)
                .sum(1);

        //System.out.println(counts);
        counts.writeAsCsv(outPath, "\n", " ").setParallelism(1);
         env.execute("bathwordcount");
    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}


