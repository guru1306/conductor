package com.netflix.conductor.dao.mongo;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ExecutionDAO;

public class MongoExecutionDAO extends BaseMongoDAO implements ExecutionDAO{
	
	

	@Inject
	public MongoExecutionDAO( ObjectMapper objectMapper, Configuration config) {
		super(objectMapper, config);
	}

	@Override
	public List<Task> getPendingTasksByWorkflow(String taskName, String workflowId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Task> getTasks(String taskType, String startKey, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Task> createTasks(List<Task> tasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateTask(Task task) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean exceedsInProgressLimit(Task task) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeTask(String taskId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Task getTask(String taskId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Task> getTasks(List<String> taskIds) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Task> getPendingTasksForTaskType(String taskType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Task> getTasksForWorkflow(String workflowId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createWorkflow(Workflow workflow) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String updateWorkflow(Workflow workflow) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeWorkflow(String workflowId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removeFromPendingWorkflow(String workflowType, String workflowId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Workflow getWorkflow(String workflowId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Workflow getWorkflow(String workflowId, boolean includeTasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getRunningWorkflowIds(String workflowName, int version) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Workflow> getPendingWorkflowsByType(String workflowName, int version) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPendingWorkflowCount(String workflowName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getInProgressTaskCount(String taskDefName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Workflow> getWorkflowsByCorrelationId(String workflowName, String correlationId, boolean includeTasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean canSearchAcrossWorkflows() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addEventExecution(EventExecution ee) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void updateEventExecution(EventExecution ee) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeEventExecution(EventExecution ee) {
		// TODO Auto-generated method stub
		
	}
	
	
	

}
