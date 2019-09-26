package com.globalegrow.tianchi.test.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author zhougenggeng createTime  2019/7/19
 */
public class KafkaProduceMoker {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "10.4.4.80:9092");
        // 等待所有副本节点的应答
        props.put("acks", "1");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 增加服务端请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String af_add_to_wishlist = JsonMocker.initEventLog("af_add_to_wishlist");
        for (int i = 0; i < 5000000; i++) {
             Thread.sleep(100);
            kafkaProducer.send(new ProducerRecord<String, String>("zaful_zz_test", af_add_to_wishlist), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (metadata != null) {

                        System.err.println(metadata.partition() + "---" + metadata.offset());
                    }
                }
            });
        }

        kafkaProducer.close();
    }



}



