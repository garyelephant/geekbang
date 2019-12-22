package org.example;

import com.google.gson.Gson;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;

public class Example {

    public static void main(String[] args) {

        System.out.println("A MongoDB Example: ");

        System.out.println("\n--- insert : ");
        final String uuid = UUID.randomUUID().toString();
        MyBean myBean = new MyBean();
        myBean.setField(uuid);
        myBean.setValue("my_value");
        final MyDao myDao = new MyDao();

        myDao.insertOne(myBean);

        System.out.println("\n--- select : ");

        Bson query = eq("field", uuid);
        MyBean result = myDao.findOne(query);
        System.out.println("result: " + result.toString());

        System.out.println("\n--- update : ");
        Bson updateQuery = eq("field", uuid);
        result.setValue("value is updated !!!");

        Bson updateDoc = new Document("$set", myDao.fromPOJO2doc(result));
        UpdateResult updateResult = myDao.coll.updateOne(updateQuery, updateDoc);
        System.out.println("update success ? " + updateResult.wasAcknowledged());
    }

    public static class MyDao extends MongoDao<MyBean> {

        private static final String MONGO_URL = "mongodb://<user>:<passwd>@<ip>:<port>";
        private static final String DATABASE = "<db_name>";
        private static final String COLLECTION = "<collection_name>";

        public MyDao() {

            super(MONGO_URL, DATABASE, COLLECTION, MyBean.class);
        }
    }

    public static class MyBean {
        private String field;
        private String value;

        public void setField(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {

            Gson gson = new Gson();
            return gson.toJson(this);

        }
    }
}
