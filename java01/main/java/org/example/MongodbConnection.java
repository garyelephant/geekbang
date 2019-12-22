package org.example;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

public class MongodbConnection {

    private MongoDatabase database;

    private MongoClient mongoClient;

    public MongodbConnection(final String mongoUrl, final String db) {
        MongoClientOptions.Builder optionBuilder = new MongoClientOptions.Builder();
        optionBuilder.cursorFinalizerEnabled(true);
        optionBuilder.connectionsPerHost(100);
        optionBuilder.connectTimeout(30000);
        optionBuilder.maxWaitTime(5000); 
        optionBuilder.socketTimeout(0);
        optionBuilder.maxConnectionIdleTime(60000);
        optionBuilder.socketKeepAlive(true);
        optionBuilder.threadsAllowedToBlockForConnectionMultiplier(5000);
        optionBuilder.writeConcern(WriteConcern.SAFE);

        MongoClientURI uri = new MongoClientURI(mongoUrl,optionBuilder);
        mongoClient = new MongoClient(uri);
        database = mongoClient.getDatabase(db);
    }

    public MongodbConnection(final String mongoUrl, final String db, MongoClientOptions.Builder optionBuilder) {

        MongoClientURI uri = new MongoClientURI(mongoUrl,optionBuilder);
        mongoClient = new MongoClient(uri);
        database = mongoClient.getDatabase(db);
    }

    public void close(){
        mongoClient.close();
    }

    public MongoDatabase getDatabase(){
        return database;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.mongoClient.close();
    }

}
