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
 * Download Handouts:
 https://university.mongodb.com/static/MongoDB_2017_M101J_August/handouts/small_zips.json
 Crunching the Zipcode dataset
 Please calculate the average population of cities in California (abbreviation CA) and New York (NY) (taken together) with populations over 25,000.
 For this problem, assume that a city name that appears in more than one state represents two separate cities.
 Please round the answer to a whole number.
 Hint: The answer for CT and NJ (using this data set) is 38177.
 Please note:
 Different states might have the same city name.
 A city might have multiple zip codes.

 For this problem, we have used a subset of the data you previously used in zips.json, not the full set. For this set, there are only 200 documents (and 200 zip codes), and all of them are in New York, Connecticut, New Jersey, and California.
 You can download the handout and perform your analysis on your machine with
 mongoimport --drop -d test -c zips small_zips.json
 Note

 This is raw data from the United States Postal Service. If you notice that while importing, there are a few duplicates fear not, this is expected and will not affect your answer.


 Once you've generated your aggregation query and found your answer, select it from the choices below.
 Please use the Aggregation pipeline to solve this problem.
 */
public class Homework5_2 {

    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("test");
        MongoIterable<String> collectionsNames = database.listCollectionNames();
        MongoCollection<Document> collection = database.getCollection("zips");

//        for (String collectionsName : collectionsNames) {
//            System.out.println(collectionsName);
//        }
//        Bson protection = Projections.fields(Projections.excludeId(), Projections.include("comments"));
//
//        for (Document documetn  : collection.find().projection(protection).into(new ArrayList<Document>())) {
//            System.out.println(documetn.toJson());
//        }

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(
                        Filters.or(Filters.eq("state", "CA" ),
                                    Filters.eq("state", "NY"))),
                Aggregates.sort(Sorts.orderBy(Sorts.descending("city"))),
                Aggregates.group("$city",
                        Accumulators.sum("pop_s", "$pop"),
                        Accumulators.first("state", "$state"),
                        Accumulators.first("t", "all")),
                Aggregates.match(  Filters.gt("pop_s", 25000)),
                Aggregates.group("t", Accumulators.avg("pop_s", "$pop_s"))
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
