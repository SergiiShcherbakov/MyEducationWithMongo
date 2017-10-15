package m101j.agregation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
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
public class exam8 {


            public static void main(String[] args) {
                MongoClient c =  new MongoClient();
                MongoDatabase db = c.getDatabase("test11");
                MongoCollection<Document> animals = db.getCollection("animals11");

                Document animal = new Document("animal", "monkey");

                animals.insertOne(animal);
                animal.remove("animal");
                animal.append("animal", "cat");
                animals.insertOne(animal);
                animal.remove("animal");
                animal.append("animal", "lion");
                animals.insertOne(animal);
            }
        }
