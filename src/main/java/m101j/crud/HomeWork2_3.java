/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package m101j.crud;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class HomeWork2_3 {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("students");
        MongoCollection<Document> collection = database.getCollection("grades");


        Bson sort = Sorts.ascending("student_id", "score");
        Bson filter = Filters.eq("type", "homework" );
        List<Document> all = collection.find( filter)
                .sort(sort)
                .into(new ArrayList<Document>());

        int studentId = -500;

        for (Document cur : all) {
            if(studentId != cur.getInteger("student_id")){
                System.out.println(cur);
                studentId = cur.getInteger("student_id");
                ObjectId objectId = cur.getObjectId("_id");
                collection.deleteOne(Filters.eq("_id", objectId));
            }
        }
    }
}
