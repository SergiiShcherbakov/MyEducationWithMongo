package m101j.agregation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.mongodb.morphia.aggregation.Accumulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Sergii Shcherbakov on 09.09.2017.
 Download Handouts:
 https://university.mongodb.com/static/MongoDB_2017_M101J_August/handouts/zips.json
 Prefered Cities to Live!

 In this problem you will calculate the number of people who live in a zip code in the US where the city starts with one of the following characthers:

 B, D, O, G, N or M .
 We will take these are the prefered cities to live in (chosen by this instructor, given is special affection to this set of characters!).

 You will be using the zip code collection data set, which you will find in the 'handouts' link in this page.

 Import it into your mongod using the following command from the command line:

 mongoimport --drop -d test -c zips zips.json
 If you imported it correctly, you can go to the test database in the mongo shell and confirm that

 > db.zips.count()
 yields 29353 documents.

 The $project stage can extract the first character from any field. For example, to extract the first character from the city field, you could write this pipeline:

 db.zips.aggregate([
 {$project:
 {
 first_char: {$substr : ["$city",0,1]},
 }
 }
 ])
 Using the aggregation framework, calculate the sum total of people who are living in a zip code where the city starts with one of those possible first characters. Choose the answer below.

 You will need to probably change your projection to send more info through than just that first character. Also, you will need a filtering step to get rid of all documents where the city does not start with the select set of initial characters.*/

public class HomeworkSubstr {

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

        List<Document> pipeline = Arrays.asList(
                new Document( "$project",
                        new Document("_id", "$city" )
                                .append( "first_char",
                                        new Document ("$substr", Arrays.asList("$city", 0, 1)))
                                .append("pop", "$pop")
                                .append("all", "all")
                ),
                new Document("$match",
                        new Document("$or",
                                Arrays.asList(
                                        new Document( "first_char", "B"),
                                        new Document( "first_char", "D"),
                                        new Document( "first_char", "O"),
                                        new Document( "first_char", "G"),
                                        new Document( "first_char", "N"),
                                        new Document( "first_char", "M")
                                )
                        )
                )
                ,
                new Document("$group", new Document("_id", "$all" )
                        .append("sum_pop", new Document("$sum", "$pop"))
                 )

        );

//                Aggregates.match(  Filters.or(Filters.eq("city", "CA" ), Filters.eq("state", "NY"))),
//                Aggregates.sort(Sorts.orderBy(Sorts.descending("_id")))
//                Aggregates.group("$city", Accumulators.sum("pop_s", "$pop"),  Accumulators.first("state", "$state"),  Accumulators.first("t", "all")),
//                Aggregates.group(new Document(), Accumulators.sum("pop_s", "$pop"),  Accumulators.first("state", "$state"),  Accumulators.first("t", "all")),
//                Aggregates.match(  Filters.gt("pop_s", 25000)),
//                Aggregates.group("t", Accumulators.avg("pop_s", "$pop_s"))
//                Aggregates.unwind("$scores"),
//                Aggregates.match( Filters.not(Filters.eq( "scores.type" , "quiz"))),
//                Aggregates.group("$_id",Accumulators.first("student_id", "$student_id"),
//                        Accumulators.first("class_id", "$class_id"),
//                        Accumulators.avg("students_sum_ball", "$scores.score")),
//                Aggregates.group("$class_id",
//                        Accumulators.avg("class_sum_ball", "$students_sum_ball")),
//                Aggregates.sort(Sorts.orderBy(Sorts.descending("class_sum_ball")))

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
