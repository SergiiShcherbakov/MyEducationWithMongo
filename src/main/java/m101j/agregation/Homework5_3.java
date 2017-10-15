package m101j.agregation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Sergii Shcherbakov on 09.09.2017.
 Download Handouts:
 https://university.mongodb.com/static/MongoDB_2017_M101J_August/handouts/Small_grades_file.zip
 Who's the easiest grader on campus?
 Download the handout and mongoimport.
 The documents look like this:
 {
 "_id" : ObjectId("50b59cd75bed76f46522c392"),
 "student_id" : 10,
 "class_id" : 5,
 "scores" : [
 {
 "type" : "exam",
 "score" : 69.17634380939022
 },
 {
 "type" : "quiz",
 "score" : 61.20182926719762
 },
 {
 "type" : "homework",
 "score" : 73.3293624199466
 },
 {
 "type" : "homework",
 "score" : 15.206314042622903
 },
 {
 "type" : "homework",
 "score" : 36.75297723087603
 },
 {
 "type" : "homework",
 "score" : 64.42913107330241
 }
 ]
 }
 There are documents for each student (student_id) across a variety of classes (class_id). Note that not all students in the same class have the same exact number of assessments. Some students have three homework assignments, etc.
 Your task is to calculate the class with the best average student performance. This involves calculating an average for each student in each class of all non-quiz assessments and then averaging those numbers to get a class average. To be clear, each student's average includes only exams and homework grades. Don't include their quiz scores in the calculation.
 What is the class_id which has the highest average student performance?
 Hint/Strategy: You need to group twice to solve this problem. You must figure out the GPA that each student has achieved in a class and then average those numbers to get a class average. After that, you just need to sort. The class with the lowest average is the class with class_id=2. Those students achieved a class average of 37.6
 You can download the handout and perform your analysis on your machine with
 mongoimport --drop -d test -c grades grades.json

 Below, choose the class_id with the highest average student average.

 Please use the Aggregation pipeline to solve this problem.*/

public class Homework5_3 {

    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("test");
        MongoIterable<String> collectionsNames = database.listCollectionNames();
        MongoCollection<Document> collection = database.getCollection("grades");

//        for (String collectionsName : collectionsNames) {
//            System.out.println(collectionsName);
//        }
//        Bson protection = Projections.fields(Projections.excludeId(), Projections.include("comments"));
//
//        for (Document documetn  : collection.find().projection(protection).into(new ArrayList<Document>())) {
//            System.out.println(documetn.toJson());
//        }

        List<Bson> pipeline = Arrays.asList(
//                Aggregates.match(  Filters.or(Filters.eq("state", "CA" ), Filters.eq("state", "NY"))),
//                Aggregates.sort(Sorts.orderBy(Sorts.descending("city"))),
//                Aggregates.group("$city", Accumulators.sum("pop_s", "$pop"),  Accumulators.first("state", "$state"),  Accumulators.first("t", "all")),
//                Aggregates.group(new Document(), Accumulators.sum("pop_s", "$pop"),  Accumulators.first("state", "$state"),  Accumulators.first("t", "all")),
//                Aggregates.match(  Filters.gt("pop_s", 25000)),
//                Aggregates.group("t", Accumulators.avg("pop_s", "$pop_s"))
                Aggregates.unwind("$scores"),
                Aggregates.match( Filters.not(Filters.eq( "scores.type" , "quiz"))),
                Aggregates.group("$_id",Accumulators.first("student_id", "$student_id"),
                        Accumulators.first("class_id", "$class_id"),
                        Accumulators.avg("students_sum_ball", "$scores.score")),
                Aggregates.group("$class_id",
                        Accumulators.avg("class_sum_ball", "$students_sum_ball")),
                Aggregates.sort(Sorts.orderBy(Sorts.descending("class_sum_ball")))
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
