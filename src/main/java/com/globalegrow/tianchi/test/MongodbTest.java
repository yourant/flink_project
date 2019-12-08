package com.globalegrow.tianchi.test;

import com.globalegrow.tianchi.util.MongoDBUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * <p>description</p>
 * Author: Ding Jian
 * Date: 2019-11-15 14:41:34
 */
public class MongodbTest {
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("34.236.225.15", 27017);

        MongoDBUtils.getClient("recommender_online");
        String db = "recommender_online";
        String tableName = "scheduler_email_user_zaful_10090646";

        MongoDatabase mongoDb = mongoClient.getDatabase(db);

        MongoCollection mongoCollection = mongoDb.getCollection(tableName);

        String key = "batch_no";

        Document doc = new Document();
        doc.put("batch_no", "201911150006002391");
        FindIterable<Document> iter = mongoCollection.find(doc);
        MongoCursor<Document> mongoCursor = iter.iterator();
        if (mongoCursor.hasNext()) {
            System.out.println( mongoCursor.next());
        }

    }
}
