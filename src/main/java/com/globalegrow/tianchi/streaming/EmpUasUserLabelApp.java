package com.globalegrow.tianchi.streaming;

import com.globalegrow.tianchi.bean.EmpMQInfo;
import com.globalegrow.tianchi.transformation.EmpMQFilter;
import com.globalegrow.tianchi.transformation.EmpMQFlatMap;
import com.globalegrow.tianchi.util.MongoDBUtils;
import com.globalegrow.tianchi.util.PropertiesUtil;
import com.mongodb.BasicDBList;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <p>邮件用户标签接UAS数据APP</p>
 * Author: Ding Jian
 * Date: 2019-11-14 15:35:44
 */
public class EmpUasUserLabelApp {


    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //mongoDB
        String db = parameterTool.get("db");
        //mongoDB collection prefix
        String tablePrefix = parameterTool.get("prefix");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpointing is required for exactly-once or at-least-once guarantees
        env.enableCheckpointing(5000);

        //RabbitMQ 参数
        Properties pro = PropertiesUtil.loadProperties("config.properties");
        String rmqHost = pro.getProperty("emp.uas.rmq.host");
        String rmqPort = pro.getProperty("emp.uas.rmq.port");
        String rmqQueueName = pro.getProperty("emp.uas.rmq.queuename");
        String rmqVHost = pro.getProperty("emp.uas.rmq.vhost");
        String rmqUserName = pro.getProperty("emp.uas.rmq.username");
        String rmqPassword = pro.getProperty("emp.uas.rmq.password");


        //String rmqHost = pro.getProperty("emp.uas.rmq.host_test");
        //String rmqPort = pro.getProperty("emp.uas.rmq.port_test");
        //String rmqQueueName = pro.getProperty("emp.uas.rmq.queuename_test");
        //String rmqVHost = pro.getProperty("emp.uas.rmq.vhost_test");
        //String rmqUserName = pro.getProperty("emp.uas.rmq.username_test");
        //String rmqPassword = pro.getProperty("emp.uas.rmq.password_test");


        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rmqHost)
                .setPort(Integer.valueOf(rmqPort))
                .setVirtualHost(rmqVHost)
                .setUserName(rmqUserName)
                .setPassword(rmqPassword)
                .build();
        final DataStream<String> stream = env
                .addSource(new RMQSource<>(
                        connectionConfig,             // config for the RabbitMQ connection
                        rmqQueueName,                 // name of the RabbitMQ queue to consume
                        false,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))    // deserialization schema to turn messages into Java objects
                .setParallelism(1)                    // non-parallel source is only required for exactly-once
                .shuffle();


         //DataStream<String> stream1 = env.readTextFile("hdfs:/user/dingjian/mqtest.txt");
        // DataStream<String> stream1 = env.readTextFile("F:\\tmp\\mqtest.txt");


        stream.filter(new EmpMQFilter())
                .flatMap(new EmpMQFlatMap())

                //.keyBy("userId")
                // .timeWindow(Time.minutes(10))
                // .allowedLateness(Time.seconds(10));
                .addSink(new RichSinkFunction<EmpMQInfo>() {

                    private MongoClient client;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        client = MongoDBUtils.getClient(db);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        if (client != null) {
                            client.close();
                        }
                    }


                    @Override
                    public void invoke(EmpMQInfo empMQInfo, Context context) {

                        List<String> list = empMQInfo.getUserIdList();
                        String site = empMQInfo.getSite();
                        Integer id = empMQInfo.getId();
                        String batchNo = empMQInfo.getBatchNo();
                        Integer pushTime = empMQInfo.getPushTime();

                        String tableName = tablePrefix + "_" + site + "_" + id;
                        //list遍历的第一条数据

                        //通过批次号batchNo查询当前批次号是否新的
                        Document document = MongoDBUtils.findOneBy(client, db, tableName, batchNo);
                        //如果没有当前批次号的数据，先清空表，再保存数据
                        if (document == null) {
                            MongoDBUtils.dropCollection(client, db, tableName);
                        }


                        //通过id查询regFrom和regionCode
                        BasicDBList queryList = new BasicDBList();
                        queryList.addAll(list);
                        //默认用zaful用户信息表
                        String userInfoTable = "zaful_emp_user_info";
                        FindIterable iterable = MongoDBUtils.findDocsBy(client, db, userInfoTable, queryList);
                        MongoCursor mongoCursor = iterable.iterator();
                        List<Document> docList = new ArrayList<>();

                        while (mongoCursor.hasNext()) {
                            Document userInfo = (Document) mongoCursor.next();

                            for (String userId : list) {
                                if (userInfo != null && userInfo.getString("user_id").equals(userId)) {

                                    Integer platform = userInfo.getInteger("platform");
                                    String country_code = userInfo.getString("country_code");
                                    int regFrom = platform == null ? -99 : platform;
                                    String regionCode = country_code == null ? "unknown" : country_code;

                                    String lang = userInfo.getString("lang") == null ? "unknown" : userInfo.getString("lang");

                                    String pipeline_code = "unknown";
                                    Document doc = new Document();
                                    doc.put("batch_no", batchNo);
                                    doc.put("user_id", userId);
                                    doc.put("platform", regFrom);
                                    doc.put("country_code", regionCode);
                                    doc.put("lang", lang);
                                    doc.put("pipeline_code", pipeline_code);

                                    doc.put("push_time", pushTime);

                                    docList.add(doc);

                                    break;
                                }
                            }


                        }


                        //批量写入数据
                        if (docList.size() > 0) {
                            MongoDBUtils.insertMany(client, db, tableName, docList);
                            System.out.println("insert mongoDB SUCCESS! tableName: " + tableName + "-----batchNo: " + batchNo);
                        }
                    }


                });

        env.execute("EmpUasUserLabelApp");

    }


}




