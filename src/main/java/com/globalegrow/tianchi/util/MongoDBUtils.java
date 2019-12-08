package com.globalegrow.tianchi.util;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <p>MongoDB 工具类</p>
 * Author: Ding Jian
 * Date: 2019-11-14 18:24:00
 */
public class MongoDBUtils {


    /**
     * 获取Client连接
     *
     * @param db 库
     * @return 连接
     */
    public static MongoClient getClient(String db) {

        //测试连接mongodb,不要用户名密码
        // MongoClient mongoClient = new MongoClient("172.31.27.16", 27017);
        // MongoClient mongoClient = new MongoClient("34.236.225.15", 27017);
        // return mongoClient;

        Properties pro = PropertiesUtil.loadProperties("config.properties");
        String mongoDBAddrs = pro.getProperty("mongodb.addr.prod");
        String username = pro.getProperty("mongodb.user.prod");
        String password = pro.getProperty("mongodb.password.prod");

        List<ServerAddress> addrs = new ArrayList<>();
        String[] hostAndPorts = mongoDBAddrs.split(",");
        for (String hostAndPort : hostAndPorts) {
            String[] arr = hostAndPort.split(":");
            String host = arr[0];
            int port = Integer.parseInt(arr[1]);

            ServerAddress serverAddress = new ServerAddress(host, port);
            addrs.add(serverAddress);

        }

        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential credential = MongoCredential.createScramSha1Credential(username, db, password.toCharArray());
        List<MongoCredential> credentials = new ArrayList<>();
        credentials.add(credential);

        //通过连接认证获取MongoDB连接
        return new MongoClient(addrs, credentials);

    }


    /**
     * 根据条件查询一条记录doc
     *
     * @param db        database
     * @param tableName collection
     * @param field     字段
     * @return 一条记录doc
     */
    public static Document findOneBy(MongoClient mongoClient, String db, String tableName, String field) {

        MongoDatabase mongoDb = mongoClient.getDatabase(db);

        MongoCollection mongoCollection = mongoDb.getCollection(tableName);

        if (mongoCollection == null) {
            return null;
        }
        Document doc = new Document();
        doc.put("batch_no", field);
        FindIterable<Document> iter = mongoCollection.find(doc);
        MongoCursor<Document> mongoCursor = iter.iterator();
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            return null;
        }
    }


    /**
     * 插入一条数据
     *
     * @param db        库
     * @param tableName 集合
     * @param doc       一条记录
     */
    public static void insertOne(MongoClient mongoClient, String db, String tableName, Document doc) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
        collection.insertOne(doc);


    }

    /**
     * 删除集合
     *
     * @param db        库
     * @param tableName collection
     */
    public static void dropCollection(MongoClient mongoClient, String db, String tableName) {
        MongoDatabase mongoDb = mongoClient.getDatabase(db);
        MongoCollection mongoCollection = mongoDb.getCollection(tableName);
        //生产不能用drop方法
        mongoCollection.deleteMany(new Document());
    }

    /**
     * 批量查询文档
     *
     * @param client        连接客户端
     * @param db            库
     * @param userInfoTable 表
     * @param queryList     要查询的列表
     * @return 文档集合
     */
    public static FindIterable findDocsBy(MongoClient client, String db, String userInfoTable, BasicDBList queryList) {

        MongoDatabase mongoDb = client.getDatabase(db);

        MongoCollection mongoCollection = mongoDb.getCollection(userInfoTable);

        BasicDBObject queryCondition = new BasicDBObject();
        BasicDBObject in = new BasicDBObject("$in", queryList);
        queryCondition.put("user_id", in);

        return mongoCollection.find(queryCondition);


    }

    /**
     * 批量写入数据
     *
     * @param client    连接客户端
     * @param db        库
     * @param tableName 表
     * @param docList   插入的docs
     */
    public static void insertMany(MongoClient client, String db, String tableName, List<Document> docList) {
        MongoDatabase mongoDb = client.getDatabase(db);

        MongoCollection mongoCollection = mongoDb.getCollection(tableName);

        mongoCollection.insertMany(docList);

    }
}
