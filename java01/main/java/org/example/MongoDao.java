package org.example;

import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;


public class MongoDao<T> {

    public final MongoCollection<Document> coll;
    protected final Class<T> objClass;
    private MongoDatabase db;
    private MongodbConnection mongodbConnection;

    /**
     * Constructor.
     * */
    public MongoDao(String mongoUrl, String dbName, String collection, Class<T> objClass) {
        this.mongodbConnection = new MongodbConnection(mongoUrl, dbName);
        this.db = this.mongodbConnection.getDatabase();
        this.coll = db.getCollection(collection);
        this.objClass = objClass;
    }

    /**
     * Constructor.
     * */
    public MongoDao(MongodbConnection mongodbConnection, String collection, Class<T> objClass) {
        this.mongodbConnection = mongodbConnection;
        this.db = this.mongodbConnection.getDatabase();
        this.coll = db.getCollection(collection);
        this.objClass = objClass;
    }

    /**
     * Insert one document of class <code>T</code>.
     * */
    public void insertOne(T value) {

        Gson gson = new Gson();
        final String jsonStr = gson.toJson(value);
        Document doc = Document.parse(jsonStr);
        coll.insertOne(doc);
    }

    public void close(){
        mongodbConnection.close();
    }

    /**
     * Insert one document of class <code>T</code>.
     * */
    public void insertMany(List<T> values) {
        Gson gson = new Gson();
        List<Document> docs = new ArrayList<>();
        for(T value : values){
            final String jsonStr = gson.toJson(value);
            Document doc = Document.parse(jsonStr);
            docs.add(doc);
        }
        coll.insertMany(docs);
    }

    /**
     * Convert Document to POJO(Plain Old Java Object).
     * */
    public T doc2POJO(Document doc) {
        Gson gson = new Gson();
        T obj = gson.fromJson(gson.toJson(doc), objClass);
        return obj;
    }

    /**
     * Convert POJO(Plain Old Java Object) to Document.
     * */
    public Document fromPOJO2doc(T object) {

        Gson gson = new Gson();
        return Document.parse(gson.toJson(object));
    }

    /**
     * Find one document by query filter.
     * @return null if document cannot be found, otherwise a instance of <code>T</code>
     */
    public List<T> find(Bson bson) {
        List<T> result = new ArrayList<>();

        FindIterable<Document> docs = coll.find(bson);

        if (docs == null || docs.first() == null) {
            return null;
        } else{
            for (Document d : docs) {
                result.add(doc2POJO(d));
            }
        }
        return result;
    }

    public Iterable<Document> find(Bson bson,int limit) {
        FindIterable<Document> docs = null;

        if(bson == null){
            docs = coll.find().limit(limit);
        }else{
            docs = coll.find(bson).limit(limit);
        }

        if (docs == null || docs.first() == null) {
            return null;
        }

        return docs;
    }

    /**
     * Find one document by query filter.
     * @return null if document cannot be found, otherwise a instance of <code>T</code>
     */
    public Iterable<Document> find(Bson bson,Bson sort,int limit) {
        FindIterable<Document> docs = null;

        if(bson == null){
            docs = coll.find().sort(sort).limit(limit);
        }else{
            docs = coll.find(bson).sort(sort).limit(limit);
        }

        if (docs == null || docs.first() == null) {
            return null;
        }
        return docs;
    }

    public BulkWriteResult bulkWrite(List<WriteModel<Document>> requests){
        return coll.bulkWrite(requests);
    }

    /**
     * Find one document by query filter.
     * @return null if document cannot be found, otherwise a instance of <code>T</code>
     */
    public T findOne(Bson bson) {

        Document doc = coll.find(bson).first();

        if (doc == null) {
            return null;
        }

        return doc2POJO(doc);
    }

    public T findOne(Bson bson,Bson sort) {

        Document doc = null;

        if(bson == null){
            doc = coll.find().sort(sort).first();
        }else{
            doc = coll.find(bson).sort(sort).first();
        }

        if (doc == null) {
            return null;
        }

        return doc2POJO(doc);
    }

    public void updateByFilter(Bson filterBson, T val, boolean upsert) {
        UpdateOptions options = new UpdateOptions();
        options.upsert(upsert);
        Document modifiers = new Document();
        modifiers.append("$set", fromPOJO2doc(val));
        coll.updateOne(filterBson,modifiers,options);
    }
}
