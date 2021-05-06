package com.netflix.conductor.dao.mongo;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.RateLimitingDAO;

public class MongoRateLimitingDAO extends BaseMongoDAO implements RateLimitingDAO {

	private static final Logger logger = LoggerFactory.getLogger(MongoRateLimitingDAO.class);
	
	@Inject
	public MongoRateLimitingDAO(ObjectMapper objectMapper, Configuration config) {
		super(objectMapper, config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean exceedsRateLimitPerFrequency(Task task, TaskDef taskDef) {
		logger.info(MongoRateLimitingDAO.class.getName() + " is ready to serve");
		return false;
	}

}
