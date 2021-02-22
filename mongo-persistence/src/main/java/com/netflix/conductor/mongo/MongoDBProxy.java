package com.netflix.conductor.mongo;

import com.google.inject.Inject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.netflix.conductor.core.config.Configuration;

public class MongoDBProxy {

	private MongoClient mongoclient;
	private MongoDatabase mongoDatabase;
	private static String CONNECTION_STRING =  "mongodb://%s:%s@%s:%s";
	
	
	@Inject
	public MongoDBProxy(MongoConfiguration mongoConfiguration, Configuration configuration) {
		
		String connectionString  = String.format(CONNECTION_STRING,mongoConfiguration.getUserName(),mongoConfiguration.getPassword(),mongoConfiguration.getClusterName(), mongoConfiguration.getPort() ); 
		//retry writes?
		ConnectionString connectionStringObj = new ConnectionString(connectionString);
		MongoClientSettings clientSettings  = MongoClientSettings.builder().applyConnectionString(connectionStringObj).build();
									 
		mongoclient = MongoClients.create(clientSettings);
		mongoDatabase = mongoclient.getDatabase(mongoConfiguration.getMongoDBName());
	}


	public MongoClient getMongoclient() {
		return mongoclient;
	}


	public void setMongoclient(MongoClient mongoclient) {
		this.mongoclient = mongoclient;
	}


	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}


	public void setMongoDatabase(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
	}
	
	
	
}
