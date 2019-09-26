package com.globalegrow.tianchi.test.kafka;

import com.globalegrow.tianchi.util.PropertiesUtil;
import lombok.val;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author zhougenggeng createTime  2019/9/26
 */
public class kafka2hive {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.enableCheckpointing(5000);
        Properties pro = PropertiesUtil.loadProperties("config.properties");
        String kafka_topic = pro.getProperty("kafka_out_topics");
        String bootstrap_servers = pro.getProperty("kafka.broker.list");
        String kafka_group = pro.getProperty("kafka.group");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap_servers);
        properties.setProperty("group.id", kafka_group);
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<>(kafka_topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> dataStreamSource = see.addSource(flinkKafkaConsumer011);
        dataStreamSource.print();
        see.execute(parameterTool.get("job-name", "realtime-user-dimension-es") + System.currentTimeMillis());
    }
}
