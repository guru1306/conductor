package com.netflix.conductor.mongo.dao.module;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.dao.mongo.MongoEventHandlerDAO;
import com.netflix.conductor.dao.mongo.MongoExecutionDAO;
import com.netflix.conductor.dao.mongo.MongoMetaDataDAO;
import com.netflix.conductor.dao.mongo.MongoPollDataDAO;
import com.netflix.conductor.dao.mongo.MongoQueueDAO;
import com.netflix.conductor.dao.mongo.MongoRateLimitingDAO;
import com.netflix.conductor.mongo.MongoConfiguration;
import com.netflix.conductor.mongo.MongoDBProxy;
import com.netflix.conductor.mongo.SystemPropertiesMongoConfiguration;

public class MongoWorkflowModule extends AbstractModule{

	
	 @Override
	    protected void configure() {
	        bind(MongoConfiguration.class).to(SystemPropertiesMongoConfiguration.class);
	        bind(MongoDBProxy.class).asEagerSingleton();
	        
	        bind(MetadataDAO.class).to(MongoMetaDataDAO.class);
	        bind(ExecutionDAO.class).to(MongoExecutionDAO.class);
	        bind(RateLimitingDAO.class).to(MongoRateLimitingDAO.class);
	        bind(EventHandlerDAO.class).to(MongoEventHandlerDAO.class);
	        bind(PollDataDAO.class).to(MongoPollDataDAO.class);
	        bind(QueueDAO.class).to(MongoQueueDAO.class);
	        
	    }
}
