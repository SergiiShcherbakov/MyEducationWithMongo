package m101j.crud;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Sergii Shcherbakov on 27.08.2017.
 */
public class HomeWork3_1 {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("school");
        MongoCollection<Document> collection = database.getCollection("students");

//        Bson filter = Filters. eq("countries" , "Sweden");
        Bson projection = Projections.fields( Projections.include("scores"));
        List<Document> students = collection.find().projection(projection).into(new LinkedList<Document>());
        double scoreNumber = 1000;
        for (Document student :students) {
            List<Document> scores = (List<Document>) student.get("scores");
            System.out.println(student);
            for (Document score : scores) {
                if (score.getString("type").equals("homework") &&
                        score.getDouble("score") < scoreNumber) {
                    scoreNumber = score.getDouble("score");
                }
            }
            System.out.println(scoreNumber);
            //remove
            System.out.println(scores);
            for (int i = 0; i < scores.size(); i++) {
                Document score = scores.get(i);
                if (score.getString("type").equals("homework") &&
                        score.getDouble("score") == scoreNumber) {
                    scores.remove(i);
                    System.out.println(score);
                    break;
                }
            }
            System.out.println(scores);
            //insert new doc
            Bson filter = Filters.eq("_id", student.get("_id"));
            Bson addScores = Updates.unset ("scores");
            collection.updateOne(filter,  addScores);

            Bson addScores1 = Updates.addEachToSet("scores", scores );
            collection.updateOne(filter,  addScores1);
            scoreNumber = 1000;
        }
    }
}
