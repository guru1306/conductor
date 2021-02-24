package com.netflix.conductor.dao.mongo;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.metrics.Monitors;

public class BaseMongoDAO {

    private static final String DAO_NAME = "mongo";
    protected static final String TASK_NAME = "name"; 
    protected static final String WORKFLOW_NAME = "name";
    protected static final String WORKFLOW_VERSION = "version";
    protected static final String EVENT_NAME = "name";
    protected static final String EVENT = "event";
    
    
    
    //object mappper settings to convert mongodb json to normal json. Long will have nested format withtout this
    protected JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build(); 
    

    

    protected ObjectMapper objectMapper;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected BaseMongoDAO(ObjectMapper objectMapper, Configuration config) {        
        this.objectMapper = objectMapper;        
    }
    

    String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    <T> T readValue(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void recordMongoDaoRequests(String action) {
    	recordMongoDaoRequests(action, "n/a", "n/a");
    }

    void recordMongoDaoRequests(String action, String taskType, String workflowType) {
        Monitors.recordDaoRequests(DAO_NAME, action, taskType, workflowType);
    }

    void rrecordMongoDaoEventRequests(String action, String event) {
        Monitors.recordDaoEventRequests(DAO_NAME, action, event);
    }

    void recordRedisDaoPayloadSize(String action, int size, String taskType, String workflowType) {
        Monitors.recordDaoPayloadSize(DAO_NAME, action, StringUtils.defaultIfBlank(taskType,""), StringUtils.defaultIfBlank(workflowType,""), size);
    }
}

