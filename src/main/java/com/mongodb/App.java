package com.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;

/**
 * Created by Sergii Shcherbakov on 20.08.2017.
 */
public class App {
    public static void main(String[] args) {
        MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(100).build();
        MongoClient client = new MongoClient( new ServerAddress(), options );

        MongoDatabase db = client.getDatabase("video");

        MongoCollection<BsonDocument> collection = db.getCollection("movieDetails", BsonDocument.class);
        FindIterable<BsonDocument> bsonDocuments = collection.find();

        for (BsonDocument bsonDocument :
              bsonDocuments) {
            System.out.println(bsonDocument);
            break;
        }
    }
}
