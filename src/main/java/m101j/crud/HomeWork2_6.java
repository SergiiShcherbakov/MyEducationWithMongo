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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class HomeWork2_6 {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("video");
        MongoCollection<Document> collection = database.getCollection("movieDetails");

        Bson filter = Filters. eq("countries" , "Sweden");
        Bson projection = Projections.fields( Projections.include("countries"), Projections.excludeId());
        List<Document> documents = collection.find(filter).projection(projection).into(new LinkedList<Document>());
        int count = 0;
        for (Document d :documents) {
           List<String> countries = (List<String>) d.get("countries");
           if(countries.size() >= 2 && countries.get(1).equals("Sweden")){
            System.out.println(d);
               count ++;
           }
        }
        System.out.println(documents.size() + " from " + count + " with Sweden");
    }
}
