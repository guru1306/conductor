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
	        
	        
//	        bind(MetadataDAO.class).to(MongoMetaDataDAO.class);
//	        bind(QueueDAO.class).to(MongoQueueDAO.class);
//	        bind(ExecutionDAO.class).to(RedisExecutionDAO.class);
//	        bind(EventHandlerDAO.class).to(RedisEventHandlerDAO.class);
//	        bind(RateLimitingDAO.class).to(RedisRateLimitingDAO.class);
//	        bind(PollDataDAO.class).to(RedisPollDataDAO.class);
//	        
//	        bind(DynomiteConfiguration.class).to(SystemPropertiesDynomiteConfiguration.class);
//	        bind(JedisCommands.class).toProvider(DynomiteJedisProvider.class).asEagerSingleton();
//	        bind(JedisCommands.class)
//	                .annotatedWith(Names.named(RedisQueuesProvider.READ_CLIENT_INJECTION_NAME))
//	                .toProvider(DynomiteJedisProvider.class)
//	                .asEagerSingleton();
//	        bind(HostSupplier.class).toProvider(ConfigurationHostSupplierProvider.class);
//	        bind(TokenMapSupplier.class).toProvider(TokenMapSupplierProvider.class);
//	        bind(ShardSupplier.class).toProvider(DynoShardSupplierProvider.class);
//	        
//	        bind(ShardingStrategy.class).toProvider(RedisQueuesShardingStrategyProvider.class).asEagerSingleton();
//	        bind(RedisQueues.class).toProvider(RedisQueuesProvider.class).asEagerSingleton();
//	        bind(DynoProxy.class).asEagerSingleton();

	       
	        
	    }
}
