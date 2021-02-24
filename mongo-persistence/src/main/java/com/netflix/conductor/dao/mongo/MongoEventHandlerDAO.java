package com.netflix.conductor.dao.mongo;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.WorkflowRepairService;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.mongo.MongoDBProxy;

public class MongoEventHandlerDAO extends BaseMongoDAO implements EventHandlerDAO {

	private static String EVENT_HANDLERS = "EVENT_HANDLERS"; 
	
	
	private static final Logger logger = LoggerFactory.getLogger(WorkflowRepairService.class);
	private MongoDatabase db;
	
	@Inject
	protected MongoEventHandlerDAO(MongoDBProxy mongoProxy,ObjectMapper objectMapper, Configuration config) {
		super(objectMapper, config);
		db = mongoProxy.getMongoDatabase();
		// TODO Auto-generated constructor stub
	}

	@Override
	public void addEventHandler(EventHandler eventHandler) {
		// TODO Auto-generated method stub
		Preconditions.checkNotNull(eventHandler.getName(), "Name");
		String json = toJson(eventHandler);	
		logger.info("Creating task document {} ", json);
		Document eventDocument = Document.parse(json);
		Bson query = Filters.eq(EVENT_NAME, eventHandler.getName());
		FindOneAndReplaceOptions upsertOption = new FindOneAndReplaceOptions();
		upsertOption.upsert(true);
		db.getCollection(EVENT_HANDLERS).findOneAndReplace(query, eventDocument, upsertOption);

	}

	@Override
	public void updateEventHandler(EventHandler eventHandler) {
		// TODO Auto-generated method stub
		String json = toJson(eventHandler);		
		Document taskDocument = Document.parse(json);
		Bson query = Filters.eq(EVENT_NAME, eventHandler.getName());
		Document result = db.getCollection(EVENT_HANDLERS).findOneAndReplace(query, taskDocument);
		
		if (result != null)				
			logger.info("task is updated { }" ,result);

	}

	@Override
	public void removeEventHandler(String name) {
		// TODO Auto-generated method stub
		Bson query = Filters.eq(EVENT_NAME, name);
		Document deletedDocument = db.getCollection(EVENT_HANDLERS).findOneAndDelete(query);
		if ( deletedDocument != null) {
			logger.info(" removed the document {}", deletedDocument.toJson());
		}
	}

	@Override
	public List<EventHandler> getAllEventHandlers() {
		FindIterable<Document> taskDefinitions = db.getCollection(EVENT_HANDLERS).find( new Document( ) );
		return StreamSupport.stream(taskDefinitions.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), EventHandler.class)).collect(Collectors.toList());
	}

	@Override
	public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
		return null;
        }
	
	public EventHandler getEventHandler(String event) {
		Bson query = Filters.eq(EVENT, event);
		Document taskDocument = db.getCollection(EVENT_HANDLERS).find(query).first();
		EventHandler result = null;
		if (taskDocument != null ) {				
			result = readValue(taskDocument.toJson(jsonWriterSettings), EventHandler.class);
		}		
		return result;
		
	}

}
