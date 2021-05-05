package com.netflix.conductor.dao.mongo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.mongo.MongoDBProxy;

public class MongoExecutionDAO extends BaseMongoDAO implements ExecutionDAO{
	
	
	private static String WORKFLOW_EXECUTION_DEFS = "WORKFLOW_EXECUTION"; 
	private static String EVENT_EXECUTION = "EVENT_EXECUTION";
	
	//TODO when field name changes, the whole persistence won't work. How to solve this?
	private static final String WORKFLOW_ID = "workflowId";
	private static final String WORKFLOW_NAME = "workflowName";
	private static final String WORKFLOW_VERSION = "version";	
	private static final String WORKFLOW_TASKS = "tasks";
	private static final String WORKFLOW_TASKS_ID = "tasks.taskId";
	private static final String WORKFLOW_TASKS_TYPE = "tasks.taskType";
	private static final String WORKFLOW_TASKS_POSITIONAL = "tasks.$";
	private static final String WORKFLOW_CORRELATION_ID = "correlationId";
	private static final String WORKFLOW_CREATE_TIME = "createTime";
	
	private static final String TASK_ID = "taskId";
	private static final String TASK_DEF_NAME = "tasks.taskDefName";
	
	
	private static final String EVENT_EXECUTION_NAME = "name";
	private static final String EVENT_EXECUTION_MESSAGE_ID = "messageId";
	private static final String EVENT_EXECUTION_EVENT = "event";
	
	
	private MongoDatabase db;
	

	@Inject
	public MongoExecutionDAO( MongoDBProxy mongoProxy, ObjectMapper objectMapper, Configuration config) {
		super(objectMapper, config);
		db = mongoProxy.getMongoDatabase();
	}

	@Override
	public List<Task> getPendingTasksByWorkflow(String taskName, String workflowId) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getPendingTasksByWorkflow");
		// TODO Auto-generated method stub
		List<Task> allTasks = getTasksForWorkflow(workflowId);
		List<Task> pendingTasksByName = new ArrayList<Task>();
		for(Task task:allTasks) {
			if ( task.getTaskDefinition().isPresent() && task.getTaskDefName().equalsIgnoreCase(taskName) && !task.getStatus().isTerminal()) {
				pendingTasksByName.add(task);
			}
		}
		return pendingTasksByName;
	}

	@Override
	public List<Task> getTasks(String taskType, String startKey, int count) {
		//TODO  For RESTAPI navigation  can be implemented later
		return null;
	}

	@Override
	public List<Task> createTasks(List<Task> tasks) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "createTasks");
		List<Task> result = new ArrayList<Task>();
		for(Task task:tasks)
		{
			logger.info("Create task {} under workflow {}", task.getTaskId(),task.getWorkflowInstanceId());
			validate(task);
			if(task.getStatus() != null && !task.getStatus().isTerminal() && task.getScheduledTime() == 0){
				task.setScheduledTime(System.currentTimeMillis());
			}
			insertOrUpdateTask(task);
			
			result.add(task);
		}
		
		return result;
		
	}
	
	
	@Override
	public void updateTask(Task task) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "updateTask");
		logger.info("Update task {} under workflow {}", task.getTaskId(),task.getWorkflowInstanceId());
		validate(task);
		insertOrUpdateTask(task);
		
	}

	private void insertOrUpdateTask(Task task) {
		String json = toJson(task);
		Bson query = Filters.and(Filters.eq(WORKFLOW_ID, task.getWorkflowInstanceId()), Filters.eq(WORKFLOW_TASKS_ID, task.getTaskId()));
				
		Document foundDoc = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).first();
		UpdateResult updateResult = null;
		if (foundDoc == null) {	
			logger.info("Task not found. Inserting the  task {} bound to  workflow {} ",task.getTaskId(),task.getWorkflowInstanceId());
			Bson findrootDocquery = Filters.eq(WORKFLOW_ID, task.getWorkflowInstanceId());
			updateResult = db.getCollection(WORKFLOW_EXECUTION_DEFS).updateOne(findrootDocquery, Updates.push(WORKFLOW_TASKS, Document.parse(json)));
		} else
		{
			logger.info("Task found. Updating the  task {} bound to  workflow {} ",task.getTaskId(),task.getWorkflowInstanceId());
			updateResult = db.getCollection(WORKFLOW_EXECUTION_DEFS).updateOne(query, Updates.set(WORKFLOW_TASKS_POSITIONAL, Document.parse(json)));
		}
		 
		logger.debug("updateResult {} for  insertOrUpdateTask task {} under workflow {}", updateResult, task.getTaskId(),task.getWorkflowInstanceId());
	
		if ( !updateResult.wasAcknowledged() ) {
			String errorMsg = String.format("not acknowledged  for task %s", task.getTaskId() );
			throw new ApplicationException(Code.CONFLICT, errorMsg);
		}
	}

	@Override
	public boolean exceedsInProgressLimit(Task task) {
		// TODO Can be implemented later
		return false;
	}

	@Override
	public boolean removeTask(String taskId) {
		
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "removeTask");
		logger.info("Remove task {}", taskId);		
		Bson query = Filters.eq(WORKFLOW_TASKS_ID, taskId);
		
		Document removeDocument = new Document(TASK_ID, taskId);		
		UpdateResult updatedResult = db.getCollection(WORKFLOW_EXECUTION_DEFS).updateOne(query, Updates.pull(WORKFLOW_TASKS, removeDocument));
		
		if (updatedResult.getModifiedCount() != 1) {
			String errorMsg = String.format("Modified unexpected documents. Update result %s ", updatedResult);
			throw new ApplicationException(Code.CONFLICT, errorMsg);
			
		}else{
			logger.info("Modified Expected documents");
		}
		return false;
	}

	@Override
	public Task getTask(String taskId) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getTask");
		logger.info("getTask  id  {}", taskId);
		Bson query = Filters.eq(WORKFLOW_TASKS_ID, taskId);
		Bson innerArrayProjection = Filters.eq(TASK_ID, taskId);
		Document  workflowDocument = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).projection(Projections.elemMatch(WORKFLOW_TASKS, innerArrayProjection)).first();
		if (workflowDocument == null) {
			logger.error("No matching task found");
			return null;
		}
		
		Workflow workflow = readValue( workflowDocument.toJson(jsonWriterSettings), Workflow.class);
		return workflow.getTasks().get(0); 
		
	}

	@Override
	public List<Task> getTasks(List<String> taskIds) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getTasks");
		
		logger.info("getTasks {}", taskIds);
		List<Task> result = new ArrayList<Task>();
		Bson query = Filters.in(WORKFLOW_TASKS_ID, taskIds);
		//TODO If we can get only nested documents, it's a much better approach.
		FindIterable<Document>  workflowDocuments = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query);		
		MongoCursor<Document> itr = workflowDocuments.iterator();
	
		Set<String> taskIdSet = new HashSet<String>();
		taskIdSet.addAll(taskIds);
		
		while (itr.hasNext()) {
			Workflow workflow = readValue(itr.next().toJson(jsonWriterSettings), Workflow.class);			
			logger.info("Found matching worflow with ID {}", workflow.getWorkflowId());
			
			for(Task task:workflow.getTasks())
			{
				if(taskIdSet.contains(task.getTaskId())) {
					logger.info("Found matching task with ID {}", task.getTaskId());
					result.add(task);
				}
			}
		}			
		
		return result;
		
	}

	@Override
	public List<Task> getPendingTasksForTaskType(String taskType) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getPendingTasksForTaskType");
		List<Task> pendingTasks = new ArrayList<Task>();
		logger.info("getPendingTasksForTaskType {}", taskType);
		Bson query = Filters.eq(WORKFLOW_TASKS_TYPE, taskType);
		FindIterable<Document> iterable = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query);
		MongoCursor<Document> itr = iterable.cursor();		
		while(itr.hasNext()) {
			Document workflowDocument = itr.next();
			Workflow workflow = readValue(workflowDocument.toJson(jsonWriterSettings), Workflow.class);			
			for(Task task:workflow.getTasks()) {
				
				if(!task.getStatus().isTerminal())
					pendingTasks.add(task);
			}			
		}		
		return pendingTasks;		
	}

	@Override
	public List<Task> getTasksForWorkflow(String workflowId) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getTasksForWorkflow");
		logger.info("getTasksForWorkflow id {}", workflowId);
		Bson query = Filters.eq(WORKFLOW_ID, workflowId);
		Document  workflowDocument = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).first();		
		Workflow workflow = readValue( workflowDocument.toJson(jsonWriterSettings), Workflow.class);		
		logger.info("Found worklow {}", workflow);
		return workflow.getTasks();		
	}

	@Override
	public String createWorkflow(Workflow workflow) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "createWorkflow");
		String json = toJson(workflow);
		Document workflowDocument = Document.parse(json);
		db.getCollection(WORKFLOW_EXECUTION_DEFS).insertOne(workflowDocument);
		return workflow.getWorkflowId();
	}

	@Override
	public String updateWorkflow(Workflow workflow) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "updateWorkflow");
		String json = toJson(workflow);
		
		Bson query = Filters.eq(WORKFLOW_ID, workflow.getWorkflowId());
		Document workflowIncomingDocument = Document.parse(json);		
		workflowIncomingDocument.remove(WORKFLOW_TASKS);//Updating all items except tasks. Looks like a hack to me		
		logger.info("updateWorkflow incoming document after modification {}", workflowIncomingDocument);
		List<Bson> updates = new ArrayList<Bson>();
		for (Entry<String, Object> updateEntry:workflowIncomingDocument.entrySet()) {
			Bson update = Updates.set(updateEntry.getKey(), updateEntry.getValue());
			updates.add(update);
		}
		Document  workflowDocument = db.getCollection(WORKFLOW_EXECUTION_DEFS).findOneAndUpdate(query, Updates.combine(updates));	
		
		if ( workflowDocument != null) {
			Workflow updatedWorkflow = readValue( workflowDocument.toJson(jsonWriterSettings), Workflow.class);
			return updatedWorkflow.getWorkflowId();
		}
		
		logger.info("Failed to update workflow with ID {}", workflow.getWorkflowId());
		return  null;
	}

	@Override
	public boolean removeWorkflow(String workflowId) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "removeWorkflow");
		Bson query = Filters.eq(WORKFLOW_ID, workflowId);
		Document  workflowDocument = db.getCollection(WORKFLOW_EXECUTION_DEFS).findOneAndDelete(query);	
		if (workflowDocument != null) {
			logger.info("Successfully removed workflow document");
			return true;
		}
			
		return false;
	}

	@Override
	public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
		// TODO not implemented
		return false;
	}

	@Override
	public void removeFromPendingWorkflow(String workflowType, String workflowId) {
		//TODO Need to think on this state
		return;
	}

	@Override
	public Workflow getWorkflow(String workflowId) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getWorkflow");
		Bson query = Filters.eq(WORKFLOW_ID, workflowId);
		Document  workflowDocument = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).first();	
		if ( workflowDocument != null) {
			Workflow workflowFound = readValue( workflowDocument.toJson(jsonWriterSettings), Workflow.class);
			logger.info("getWorkflow Found workflow with ID {}", workflowFound );
			return workflowFound;
		}
		
		
		return  null;
	}

	@Override
	public Workflow getWorkflow(String workflowId, boolean includeTasks) {		
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getWorkflowWithTasks="+includeTasks );
		
		Bson query = Filters.eq(WORKFLOW_ID, workflowId);
		Document  workflowDocument = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).first();	
		if ( workflowDocument != null) {
			Workflow workflowFound = readValue( workflowDocument.toJson(jsonWriterSettings), Workflow.class);
			if (!includeTasks) {
				workflowFound.setTasks(new ArrayList<Task>());
			}
			logger.info("getWorkflow Found workflow with ID {}", workflowFound );
			return workflowFound;
		}
		
		return null;
	}

	@Override
	public List<String> getRunningWorkflowIds(String workflowName, int version) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getRunningWorkflowIds");
		List<Workflow> pendingWorkflows = getPendingWorkflowsByType(workflowName, version);
		return pendingWorkflows.stream().map(Workflow::getWorkflowId).collect(Collectors.toList());
	}

	@Override
	public List<Workflow> getPendingWorkflowsByType(String workflowName, int version) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getPendingWorkflowsByType");
		List<Workflow> runningWorkflows = new ArrayList<>();
		Bson query = Filters.and( Filters.eq(WORKFLOW_NAME, workflowName) , Filters.eq(WORKFLOW_VERSION, version) );
		FindIterable<Document>  workflowDocuments = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query);		
		MongoCursor<Document> itr = workflowDocuments.cursor();
		//TODO use stream support and split iterator.
		while( itr.hasNext() )	{
			Document returnedDoc = itr.next();
			Workflow workflow = readValue(returnedDoc.toJson(jsonWriterSettings), Workflow.class);
			logger.info("getPendingWorkflows workflow {}", workflow);
			if( !workflow.getStatus().isTerminal() ) {
				runningWorkflows.add( workflow );
			}
		}
		return runningWorkflows;
	}

	@Override
	public long getPendingWorkflowCount(String workflowName) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getPendingWorkflowCount");
		//TODO Need better ways to speed up the fetching. Directly use not completed, not terminated.
		long count = 0;
		Bson query = Filters.eq(WORKFLOW_NAME, workflowName);
		FindIterable<Document>  workflowDocuments = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query);		
		MongoCursor<Document> itr = workflowDocuments.cursor();
		//TODO use stream support and split iterator.
		while( itr.hasNext() )	{
			Document returnedDoc = itr.next();
			Workflow workflow = readValue(returnedDoc.toJson(jsonWriterSettings), Workflow.class);			
			logger.info("getPendingWorkflowCount workflow {}", workflow);
			if( !workflow.getStatus().isTerminal() ) {
				count++;
			}
		}
		return count;
	}
	

	@Override
	public long getInProgressTaskCount(String taskDefName) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getInProgressTaskCount");
		long count = 0;
		logger.info("getInProgressTaskCount  taskDefName  {}", taskDefName);
		Bson query = Filters.eq(TASK_DEF_NAME, taskDefName);		
		FindIterable<Document>  workflowDocuments = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).projection(Projections.include(WORKFLOW_TASKS));
		MongoCursor<Document> cursorDocument = workflowDocuments.cursor();
		//TODO use aggregation queries to get only matched tasks
		while( cursorDocument.hasNext() ) {
			Workflow workflow = readValue( cursorDocument.next().toJson(jsonWriterSettings), Workflow.class);
			for(Task task:workflow.getTasks())
			{
				if (task.getTaskDefinition().isPresent() && task.getTaskDefName().equals(taskDefName) && !task.getStatus().isTerminal()) {
					count++;
				}
			}
		}
		
		
		return count;
	}

	@Override
	public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getWorkflowsByType");
		Bson nameEquals = Filters.eq(WORKFLOW_NAME, workflowName);
		Bson startTimeGTE = Filters.gte(WORKFLOW_CREATE_TIME, startTime);
		Bson endTimeLTE = Filters.lte(WORKFLOW_CREATE_TIME, endTime);		
		Bson query = Filters.and(nameEquals, startTimeGTE, endTimeLTE);
		FindIterable<Document> docsItr = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(query).projection(Projections.exclude(WORKFLOW_TASKS));		
		
		return StreamSupport.stream(docsItr.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), Workflow.class)).collect(Collectors.toList());
				
		
	}

	@Override
	public List<Workflow> getWorkflowsByCorrelationId(String workflowName, String correlationId, boolean includeTasks) {
		recordMongoDaoEventRequests(WORKFLOW_EXECUTION_DEFS, "getWorkflowsByCorrelationId");
		
		Bson findWorkflowByCorrelationQuery = Filters.and(Filters.eq(WORKFLOW_NAME, workflowName), Filters.eq(WORKFLOW_CORRELATION_ID,correlationId));
		FindIterable<Document> docsItr = db.getCollection(WORKFLOW_EXECUTION_DEFS).find(findWorkflowByCorrelationQuery);
		return StreamSupport.stream(docsItr.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), Workflow.class)).collect(Collectors.toList());	
	}

	@Override
	public boolean canSearchAcrossWorkflows() {
		return true;
	}

	@Override
	public boolean addEventExecution(EventExecution ee) {
		logger.info("addEventExecution ee {}", ee );
		String json = toJson(ee);
		
		Bson name = Filters.eq(EVENT_EXECUTION_NAME,ee.getName());
		Bson event = Filters.eq(EVENT_EXECUTION_EVENT, ee.getEvent());
		Bson messageId = Filters.eq(EVENT_EXECUTION_MESSAGE_ID, ee.getMessageId());		
		Bson createQuery = Filters.and(name,event,messageId);
		
		Bson update = Updates.setOnInsert(Document.parse(json));
		
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();		
		options.upsert(true);
		
		Document updatedDoc = db.getCollection(EVENT_EXECUTION).findOneAndUpdate(createQuery, update, options);
		
		if ( updatedDoc == null ) {
			logger.info("Created new event execution {}", ee);
			return true;			
		}
		
		return false;
	}

	@Override
	public void updateEventExecution(EventExecution ee) {
		logger.info("updateEventExecution ee {}", ee );
		String payload = toJson(ee);
		
		Bson name = Filters.eq(EVENT_EXECUTION_NAME,ee.getName());
		Bson event = Filters.eq(EVENT_EXECUTION_EVENT, ee.getEvent());
		Bson messageId = Filters.eq(EVENT_EXECUTION_MESSAGE_ID, ee.getMessageId());		
		Bson findQuery = Filters.and(name,event,messageId);		
		
		Document eventExecutionDoc = Document.parse(payload);
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.upsert(true);
		
		List<Bson> updates = new ArrayList<Bson>();
		for (Entry<String, Object> updateEntry:eventExecutionDoc.entrySet()) {
			Bson update = Updates.set(updateEntry.getKey(), updateEntry.getValue());
			updates.add(update);
		}
		
		Document updatedDoc = db.getCollection(EVENT_EXECUTION).findOneAndUpdate(findQuery, Updates.combine(updates), options);
		if (updatedDoc != null) {
			logger.info("Updated existing document {}", updatedDoc);
		}
		
	}

	@Override
	public void removeEventExecution(EventExecution ee) {
		logger.info("removeEventExecution ee {}", ee );
		
		
		Bson name = Filters.eq(EVENT_EXECUTION_NAME,ee.getName());
		Bson event = Filters.eq(EVENT_EXECUTION_EVENT, ee.getEvent());
		Bson messageId = Filters.eq(EVENT_EXECUTION_MESSAGE_ID, ee.getMessageId());		
		Bson findQuery = Filters.and(name,event,messageId);
		
		
		Document deletedDoc = db.getCollection(EVENT_EXECUTION).findOneAndDelete(findQuery);
		if (deletedDoc != null) {
			logger.info("Deleted existing document {}", deletedDoc);
		}
		
	}
	
	
	private void validate(Task task) {
	    try {
            Preconditions.checkNotNull(task, "task object cannot be null");
            Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
            Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
            Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");
        } catch (NullPointerException npe){
	        throw new ApplicationException(Code.INVALID_INPUT, npe.getMessage(), npe);
        }
    }
	
	

}
