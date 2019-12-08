package com.globalegrow.tianchi.test.wordcount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * <p>description</p>
 * Author: Ding Jian
 * Date: 2019-09-27 16:45:55
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("E:/flinkData/input");
        String outputPath = "E:/flinkData/output";
        System.out.println("aaaaa");

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        counts.writeAsCsv(outputPath, "\n", " ");
        env.execute("wc");
    }
}
// User-defined functions
        class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

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


