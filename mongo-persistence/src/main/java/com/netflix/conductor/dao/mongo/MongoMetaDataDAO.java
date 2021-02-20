package com.netflix.conductor.dao.mongo;

import java.util.List;
import java.util.Optional;

import com.google.inject.Inject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.mongo.MongoDBProxy;

public class MongoMetaDataDAO implements MetadataDAO {
	
	private static String COLLECTION_NAME = "WORKFLOW_META_DEFS"; 
	private MongoDatabase db;
	private MongoClient client;
	
	@Inject
	public MongoMetaDataDAO(MongoDBProxy mongoProxy) {
		db = mongoProxy.getMongoDatabase();
		client = mongoProxy.getMongoclient();
	}

	@Override
	public void createTaskDef(TaskDef taskDef) {
		

	}

	@Override
	public String updateTaskDef(TaskDef taskDef) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskDef getTaskDef(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TaskDef> getAllTaskDefs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeTaskDef(String name) {
		// TODO Auto-generated method stub

	}

	@Override
	public void createWorkflowDef(WorkflowDef def) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateWorkflowDef(WorkflowDef def) {
		// TODO Auto-generated method stub

	}

	@Override
	public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeWorkflowDef(String name, Integer version) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<WorkflowDef> getAllWorkflowDefs() {
		// TODO Auto-generated method stub
		return null;
	}

}
