package com.globalegrow.tianchi.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object MykafkaUtil {

  private val properties = new Properties()
  properties.setProperty("bootstrap.servers","hadoop102:9092")
  properties.setProperty("group.id","globale")




  def getKafkaConsumer(topic:String ): FlinkKafkaConsumer011[String] ={
        new FlinkKafkaConsumer011[String](topic ,new SimpleStringSchema(),properties)
   }

  def getKafkaProducer(topic:String):FlinkKafkaProducer011[String]={
    new FlinkKafkaProducer011[String](properties.getProperty("bootstrap.servers"),topic,new SimpleStringSchema())
  }

}
