package com.netflix.conductor.mongo.dao.module;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.dao.dynomite.RedisEventHandlerDAO;
import com.netflix.conductor.dao.dynomite.RedisExecutionDAO;
import com.netflix.conductor.dao.dynomite.RedisPollDataDAO;
import com.netflix.conductor.dao.dynomite.RedisRateLimitingDAO;
import com.netflix.conductor.dao.dynomite.queue.DynoQueueDAO;
import com.netflix.conductor.dao.mongo.MongoEventHandlerDAO;
import com.netflix.conductor.dao.mongo.MongoExecutionDAO;
import com.netflix.conductor.dao.mongo.MongoMetaDataDAO;
import com.netflix.conductor.dao.mongo.MongoPollDataDAO;
import com.netflix.conductor.dyno.DynoProxy;
import com.netflix.conductor.dyno.DynoShardSupplierProvider;
import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.conductor.dyno.RedisQueuesProvider;
import com.netflix.conductor.dyno.RedisQueuesShardingStrategyProvider;
import com.netflix.conductor.dyno.SystemPropertiesDynomiteConfiguration;
import com.netflix.conductor.jedis.ConfigurationHostSupplierProvider;
import com.netflix.conductor.jedis.DynomiteJedisProvider;
import com.netflix.conductor.jedis.TokenMapSupplierProvider;
import com.netflix.conductor.mongo.MongoConfiguration;
import com.netflix.conductor.mongo.MongoDBProxy;
import com.netflix.conductor.mongo.SystemPropertiesMongoConfiguration;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.redis.RedisQueues;
import com.netflix.dyno.queues.redis.sharding.ShardingStrategy;

import redis.clients.jedis.commands.JedisCommands;

public class MongoWorkflowModule extends AbstractModule{

	
	 @Override
	    protected void configure() {
	        bind(MetadataDAO.class).to(MongoMetaDataDAO.class);
	        bind(MongoConfiguration.class).to(SystemPropertiesMongoConfiguration.class);
	        bind(MongoDBProxy.class).asEagerSingleton();
	        bind(ExecutionDAO.class).to(MongoExecutionDAO.class);
	        bind(RateLimitingDAO.class).to(RedisRateLimitingDAO.class);
	        bind(EventHandlerDAO.class).to(MongoEventHandlerDAO.class);
	        bind(PollDataDAO.class).to(MongoPollDataDAO.class);
	        bind(QueueDAO.class).to(DynoQueueDAO.class);
	        
	        bind(DynomiteConfiguration.class).to(SystemPropertiesDynomiteConfiguration.class);
	        bind(JedisCommands.class).toProvider(DynomiteJedisProvider.class).asEagerSingleton();
	        bind(JedisCommands.class)
	                .annotatedWith(Names.named(RedisQueuesProvider.READ_CLIENT_INJECTION_NAME))
	                .toProvider(DynomiteJedisProvider.class)
	                .asEagerSingleton();
	        bind(HostSupplier.class).toProvider(ConfigurationHostSupplierProvider.class);
	        bind(TokenMapSupplier.class).toProvider(TokenMapSupplierProvider.class);
	        bind(ShardSupplier.class).toProvider(DynoShardSupplierProvider.class);
	        
	        bind(ShardingStrategy.class).toProvider(RedisQueuesShardingStrategyProvider.class).asEagerSingleton();
	        bind(RedisQueues.class).toProvider(RedisQueuesProvider.class).asEagerSingleton();
	        bind(DynoProxy.class).asEagerSingleton();

	       
	        
	    }
}
