package com.netflix.conductor.mongo;

import com.netflix.conductor.core.config.Configuration;

public interface MongoConfiguration extends Configuration {
	//Holds the configuration for mongodb
	
	String MONGODB_USERNAME = "conductor.db.mongo.username";
	String MONGODB_USERNAME_DEFAULT_VALUE = "null";
	
	String MONGODB_CLUSTER_NAME = "conductor.cluster.mongo.name";
	String MONGODB_CLUSTERNAME_DFAULT_VALUE = "null";
	
	String MONGODB_PASSWORD = "conductor.db.mongo.password";
	String MONGODB_PASSWORD_DEFAULT_VALUE = "null";
	
	String MONGODB_PORT= "conductor.db.mongo.port";
	int MONGODB_PORT_VALUE = 27017; 
	
	String MONGODB_NAME = "conductor.db.mongo.name";
	String MONGODDB_NAME_VALUE = "conductor";
	
	default String getUserName() {
		return getProperty(MONGODB_USERNAME, MONGODB_USERNAME_DEFAULT_VALUE);
	}
	
	default String getPassword() {
		return getProperty(MONGODB_PASSWORD, MONGODB_PASSWORD_DEFAULT_VALUE);
	}
	
	default String getClusterName() {
		return getProperty(MONGODB_CLUSTER_NAME, MONGODB_CLUSTERNAME_DFAULT_VALUE);
	}
	
	default int getPort() {
		return getIntProperty(MONGODB_PORT, MONGODB_PORT_VALUE);
	}	
	
	default String getMongoDBName() {
		return getProperty(MONGODB_NAME, MONGODDB_NAME_VALUE);
	}
	

}
