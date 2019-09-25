package com.globalegrow.tianchi.test.wordcount

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object BatchWcApp {


  def main(args: Array[String]): Unit = {
    val inputPath: String = "file:///d:/tmp/hello.txt"


    //  env => source => transform=> sink
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //source
    val dataSet: DataSet[String] = env.readTextFile(inputPath)
    //transform
    // 其中flatMap 和Map 中  需要引入隐式转换
    //经过groupby进行分组，sum进行聚合
    import org.apache.flink.api.scala.createTypeInformation

    val aggDataSet: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    //sink
    aggDataSet.print()


  }

}
