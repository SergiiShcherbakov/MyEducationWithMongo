package m101j.agregation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * Created by Sergii Shcherbakov on 09.09.2017.
 *
 */
public class exam4_understand {


            public static void main(String[] args) {
                MongoClient c =  new MongoClient();

                MongoDatabase db = c.getDatabase("blog");
                MongoCollection<Document> imagesCollection = db.getCollection("posts");
                System.out.println( imagesCollection.find().first());


            }
        }
