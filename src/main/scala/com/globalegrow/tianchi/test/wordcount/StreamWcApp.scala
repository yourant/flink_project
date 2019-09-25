package com.globalegrow.tianchi.test.wordcount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWcApp {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dstream: DataStream[String] = env.socketTextStream("hadoop103",7777)

    val wcDstream: DataStream[(String, Int)] = dstream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    wcDstream.print().setParallelism(1)

    env.execute()
  }

}
