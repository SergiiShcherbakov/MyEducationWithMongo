package m101j.agregation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Sergii Shcherbakov on 09.09.2017.
 */
public class Homework5_1 {

    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("blog");
        MongoIterable<String> collectionsNames = database.listCollectionNames();
        MongoCollection<Document> collection = database.getCollection("posts");

//        for (String collectionsName : collectionsNames) {
//            System.out.println(collectionsName);
//        }
//        Bson protection = Projections.fields(Projections.excludeId(), Projections.include("comments"));
//
//        for (Document documetn  : collection.find().projection(protection).into(new ArrayList<Document>())) {
//            System.out.println(documetn.toJson());
//        }

        List<Bson> pipeline = Arrays.asList(
                Aggregates.project(
                        Projections.fields(
                                Projections.include("comments"),
                                Projections.excludeId())),
                Aggregates.unwind("$comments"),
                Aggregates.group("$comments.author",
                        Accumulators.sum("countComments", 1)),
                Aggregates.sort(Sorts.orderBy(Sorts.descending("countComments")))
 );

        List<Document> result = collection.aggregate(pipeline).into(new ArrayList<Document>());
        for (Document document : result) {
            System.out.println(document.toJson());
        }
//        -----------------------------
//        db.zipcodes.aggregate( [
//                { $group: { _id: "$state", totalPop: { $sum: "$pop" } } }
//                ] )
//        List<Document> pipeline = Arrays.asList(
//                new Document("$group",
//                        new Document("_id", "$state").append("totalPop",
//                                new Document("$sum","$pop"))
//                                 )
//        );
//
//        List<Document> result = collection.aggregate(pipeline).into(new ArrayList<Document>());
//      -------------------------------------
//        db.zipcodes.aggregate( [
//                { $group: { _id: "$state", totalPop: { $sum: "$pop" } } },
//        { $match: { totalPop: { $gte: 10*1000*1000 } } }
//                ] )
//        List<Document> pipeline = Arrays.asList(
//                new Document("$group",
//                        new Document("_id", "$state").append("totalPop",
//                                new Document("$sum","$pop"))),
//                new Document("$match",
//                        new Document("totalPop",
//                                new Document("$gte", 10000000)))
//        );
//
//        List<Document> result = collection.aggregate(pipeline).into(new ArrayList<Document>());
//      -------------------------------------

//        db.zipcodes.aggregate( [
//                { $group: { _id: "$state", totalPop: { $sum: "$pop" } } },
//        { $match: { totalPop: { $gte: 10*1000*1000 } } }
//                ] )
//        List<Bson> pipeline = Arrays.asList( Aggregates.group("$state", Accumulators.sum("totalPop", "$pop")),
//                Aggregates.match(Filters.gte("totalPop", 10000000)));
//
//        List<Document> result = collection.aggregate(pipeline).into(new ArrayList<Document>());
//      -------------------------------------


//        db.zipcodes.aggregate( [
//                { $group: { _id: "$state", totalPop: { $sum: "$pop" } } },
//        { $match: { totalPop: { $gte: 10*1000*1000 } } }
//                ] )
//        List<Document> pipeline = Arrays.asList( Document.parse("{ $group: { _id: \"$state\", totalPop: { $sum: \"$pop\" } } }"),
//                Document.parse("{ $match: { totalPop: { $gte: 10000000 } } }"));
//
//        List<Document> result = collection.aggregate(pipeline).into(new ArrayList<Document>());
//      -------------------------------------


//        List<Document> result = collection.find().into(new ArrayList<Document>());
//        for (Document document : result) {
//            System.out.println(document.toJson());
//        }

    }
}
