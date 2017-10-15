package m101j.agregation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Sergii Shcherbakov on 09.09.2017.
  */
public class exam7 {


            public static void main(String[] args) {
                MongoClient c =  new MongoClient();
                MongoDatabase db = c.getDatabase("photo");
                MongoCollection<Document> imagesCollection = db.getCollection("images");
                MongoCollection<Document> albumCollection = db.getCollection("album");

                List<Document> images = imagesCollection.find().into(new LinkedList<Document>());
                int id;
                for (Document image : images) {
                     id = image.getInteger("_id");
                     if( albumCollection.count(new Document("images", id)) == 0){
                        System.out.println("found :" + id);
                        imagesCollection.deleteOne(new Document("_id", id));
                     }
                }



            }
        }
