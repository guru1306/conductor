package com.netflix.conductor.dao.mongo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowRepairService;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.mongo.MongoDBProxy;

public class MongoPollDataDAO extends BaseMongoDAO implements PollDataDAO {

	private static final Logger logger = LoggerFactory.getLogger(WorkflowRepairService.class);
	private MongoDatabase db;
	
	@Inject
	protected MongoPollDataDAO(MongoDBProxy mongoProxy,ObjectMapper objectMapper, Configuration config) {
		super(objectMapper, config);
		db = mongoProxy.getMongoDatabase();
		// TODO Auto-generated constructor stub
	}

	private final static String POLL_DATA = "POLL_DATA";
	@Override
	public void updateLastPollData(String taskDefName, String domain, String workerId) {
		// TODO Auto-generated method stub
		Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
        PollData pollData = new PollData(taskDefName, domain, workerId, System.currentTimeMillis());
        String payload = toJson(pollData);
        Document pollDocument = Document.parse(payload);           
        Bson query = Filters.eq(TASK_NAME, taskDefName);
		FindOneAndReplaceOptions upsertOption = new FindOneAndReplaceOptions();
		upsertOption.upsert(true);
		db.getCollection(POLL_DATA).findOneAndReplace(query, pollDocument, upsertOption);
		logger.info("Poll data updated for the task",taskDefName);

	}

	@Override
	public PollData getPollData(String taskDefName, String domain) {
		String field = (domain == null) ? "DEFAULT" : domain;
		Bson query = Filters.eq(TASK_NAME, taskDefName);
		Document taskDocument = db.getCollection(POLL_DATA).find(query).first();
		PollData result = null;
		if (taskDocument != null ) {
			logger.info("getPollData",taskDocument);
			result = readValue(taskDocument.toJson(jsonWriterSettings), PollData.class);
		}		
		return result;
	}

	@Override
	public List<PollData> getPollData(String taskDefName) {
		FindIterable<Document> taskDefinitions = db.getCollection(POLL_DATA).find( new Document( ) );
		return StreamSupport.stream(taskDefinitions.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), PollData.class)).collect(Collectors.toList());
	
	}

}
