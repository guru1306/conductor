package com.netflix.conductor.dao.mongo;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowRepairService;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.mongo.MongoDBProxy;

public class MongoMetaDataDAO extends BaseMongoDAO implements MetadataDAO {
	
	private static String WORKFLOW_META_COLLECTION = "WORKFLOW_META_DEFS"; 
	private static String TASK_META_COLLECTION = "TASK_META_DEFS";
	
	
	private static final Logger logger = LoggerFactory.getLogger(WorkflowRepairService.class);
	private MongoDatabase db;
	
	
	@Inject
	public MongoMetaDataDAO(MongoDBProxy mongoProxy,ObjectMapper mapper, Configuration config) {
		super(mapper, config);
		db = mongoProxy.getMongoDatabase();		
		
	}

	@Override
	public void createTaskDef(TaskDef taskDef) {	
		String json = toJson(taskDef);	
		logger.info("Creating task document {} ", json);
		Document taskDocument = Document.parse(json);
		Bson query = Filters.eq(TASK_NAME, taskDef.getName());
		FindOneAndReplaceOptions upsertOption = new FindOneAndReplaceOptions();
		upsertOption.upsert(true);
		db.getCollection(TASK_META_COLLECTION).findOneAndReplace(query, taskDocument, upsertOption);
		
	}
	
	

	@Override
	public String updateTaskDef(TaskDef taskDef) {		
		String json = toJson(taskDef);		
		Document taskDocument = Document.parse(json);
		Bson query = Filters.eq(TASK_NAME, taskDef.getName());
		Document result = db.getCollection(TASK_META_COLLECTION).findOneAndReplace(query, taskDocument);
		
		if (result != null)				
			logger.info("task is updated { }" ,result);
		return taskDef.getName();
		
	}
	
	

	@Override
	public TaskDef getTaskDef(String name) {
		Bson query = Filters.eq(TASK_NAME, name);
		Document taskDocument = db.getCollection(TASK_META_COLLECTION).find(query).first();
		TaskDef result = null;
		if (taskDocument != null ) {				
			result = readValue(taskDocument.toJson(jsonWriterSettings), TaskDef.class);
		}		
		
		return result;
		
	}

	@Override
	public List<TaskDef> getAllTaskDefs() {
		FindIterable<Document> taskDefinitions = db.getCollection(TASK_META_COLLECTION).find( new Document( ) );
		return StreamSupport.stream(taskDefinitions.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), TaskDef.class)).collect(Collectors.toList());
		
	}

	@Override
	public void removeTaskDef(String name) {		
		Bson query = Filters.eq(TASK_NAME, name);
		Document deletedDocument = db.getCollection(TASK_META_COLLECTION).findOneAndDelete(query);
		if ( deletedDocument != null) {
			logger.info(" removed the document {}", deletedDocument.toJson());
		}		
	}

	@Override
	public void createWorkflowDef(WorkflowDef def) {
		String json = toJson(def);		
		Document workflowDocument = Document.parse(json);
		FindOneAndReplaceOptions upsertOption = new FindOneAndReplaceOptions();
		upsertOption.upsert(true);
		Bson query = Filters.and( Filters.eq(WORKFLOW_NAME, def.getName()), Filters.eq(WORKFLOW_VERSION, def.getVersion()));
		db.getCollection(WORKFLOW_META_COLLECTION).findOneAndReplace(query, workflowDocument, upsertOption);
	}

	@Override
	public void updateWorkflowDef(WorkflowDef def) {
		String json = toJson(def);	
		Document defDocument = Document.parse(json);
		Bson query = Filters.and( Filters.eq(WORKFLOW_NAME, def.getName()), Filters.eq(WORKFLOW_VERSION, def.getVersion()));
		Document workflowdef = db.getCollection(WORKFLOW_META_COLLECTION).findOneAndReplace(query, defDocument);
		if ( workflowdef != null ) {
			logger.info("workflow  is updated { }" , workflowdef);
		}
		
	}

	@Override
	public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
		Bson query = Filters.eq(WORKFLOW_NAME, name);
		FindIterable<Document> documents = db.getCollection(WORKFLOW_META_COLLECTION).find(query);
		MongoCursor<Document> itr = documents.iterator();
		int maxVersion = -1;
		WorkflowDef result = null;
		//TODO see if max function is available in MongoDB
		while (itr.hasNext())
		{
			Document document = itr.next();
			WorkflowDef def = readValue(document.toJson(jsonWriterSettings), WorkflowDef.class);
			
			if (def.getVersion() > maxVersion)
			{
				maxVersion = def.getVersion();
				result = def;
			}
		}
		
		return Optional.ofNullable(result);
		
	}

	@Override
	public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
		Bson query = Filters.and( Filters.eq(WORKFLOW_NAME, name), Filters.eq(WORKFLOW_VERSION, version) );
		Document workflowDocument = db.getCollection(WORKFLOW_META_COLLECTION).find(query).first();		 
		WorkflowDef result = null;
		
		if(workflowDocument != null) {
			result = readValue(workflowDocument.toJson(jsonWriterSettings), WorkflowDef.class);
		}
		return Optional.ofNullable(result);
	}

	@Override
	public void removeWorkflowDef(String name, Integer version) {
		
		Bson query = Filters.and( Filters.eq(WORKFLOW_NAME, name), Filters.eq(WORKFLOW_VERSION, version) );
		Document deletedDocument = db.getCollection(WORKFLOW_META_COLLECTION).findOneAndDelete(query);
		if (deletedDocument != null) {
			logger.info("Deleted document {}",deletedDocument);
		}
	}

	@Override
	public List<WorkflowDef> getAllWorkflowDefs() {
		FindIterable<Document> taskDefinitions = db.getCollection(WORKFLOW_META_COLLECTION).find( new Document( ) );
		return StreamSupport.stream(taskDefinitions.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), WorkflowDef.class)).collect(Collectors.toList());
	}

}
