package com.netflix.conductor.dao.mongo;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.mongodb.ConnectionString;
import com.mongodb.DBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.operation.OrderBy;
import com.mongodb.operation.UpdateOperation;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.WorkflowRepairService;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.mongo.MongoDBProxy;

public class MongoQueueDAO extends BaseMongoDAO implements QueueDAO {
	
	private static String QUEUE_COLLECTION = "QUEUE_DEFS";
    protected static final String QUEUE_NAME = "queue_name"; 
    protected static final String MESSAGE_ID = "message_id"; 
    protected static final String PRIORITY = "priority"; 
    protected static final String OFFSET_TIME_SECONDS = "offset_time_seconds"; 
    protected static final String DELIVER_ON = "deliver_on"; 
    protected static final String PAYLOAD = "payload"; 
    protected static final String POPPED = "popped"; 

	private static final Logger logger = LoggerFactory.getLogger(MongoQueueDAO.class);
	public MongoDatabase mongoDatabase;

	@Inject
	public MongoQueueDAO(MongoDBProxy mongoProxy, ObjectMapper objectMapper, Configuration config) {
		super(objectMapper, config);
		logger.info(MongoQueueDAO.class.getName() + " is ready to serve");
		mongoDatabase = mongoProxy.getMongoDatabase();
	}
	protected MongoQueueDAO(MongoDatabase md, ObjectMapper objectMapper) {
		super(objectMapper, null);
		mongoDatabase = md;
	}

	@Override
	public void push(String queueName, String messageID, long offsetTimeInSecond) {
		push(queueName, messageID, 0, offsetTimeInSecond);
	}

	@Override
	public void push(String queueName, String messageID, int priority, long offsetTimeInSecond) {
		pushMessage(queueName, messageID, null, priority, offsetTimeInSecond);
	}

	@Override
	public void push(String queueName, List<Message> messages) {
		// TODO Auto-generated method stub
		messages
        .forEach(message -> pushMessage(queueName, message.getId(), message.getPayload(), message.getPriority(), 0));
	}

	@Override
	public boolean pushIfNotExists(String queueName, String messageID, long offsetTimeInSecond) {
		// TODO Auto-generated method stub
        return pushIfNotExists(queueName, messageID, 0, offsetTimeInSecond);
	}

	@Override
	public boolean pushIfNotExists(String queueName, String messageID, int priority, long offsetTimeInSecond) {
		// TODO Auto-generated method stub
		if (!existsMessage(queueName, messageID)) {
            pushMessage(queueName, messageID, null, priority, offsetTimeInSecond);
            return true;
        }
        return false;
	}

	@Override
	public List<String> pop(String queueName, int count, int timeout) {
        List<Message> messages = popMessages(queueName, count, timeout);
        if(messages == null) return new ArrayList<>();
        return messages.stream().map(Message::getId).collect(Collectors.toList());
	}

	@Override
	public List<Message> pollMessages(String queueName, int count, int timeout) {
        List<Message> messages = popMessages(queueName, count, timeout);
        if(messages == null) return new ArrayList<>();
        return messages;
	}

	@Override
	public void remove(String queueName, String messageId) {
		removeMessage(queueName, messageId);
	}

	@Override
	public int getSize(String queueName) {
        logger.info("getSize of queue: '{}'", queueName);
	    Bson filter = Filters.eq(QUEUE_NAME, queueName);
		return (int) mongoDatabase.getCollection(QUEUE_COLLECTION).countDocuments(filter);
	}

	@Override
	public boolean ack(String queueName, String messageId) {
		// TODO Auto-generated method stub
		return removeMessage(queueName, messageId);
	}

	@Override
	public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        logger.info("setUnackTimeout for queue: '{}' with messageId: '{}'", queueName, messageId);
        long updatedOffsetTimeInSecond = unackTimeout / 1000;
        
	    Bson filter = Filters.eq("_id", getDocumentIDForQueueMessage(queueName, messageId));
	    List<Bson> updatePredicates = new ArrayList<Bson>();
	    updatePredicates.add(Updates.set(OFFSET_TIME_SECONDS, updatedOffsetTimeInSecond));
	    updatePredicates.add(Updates.set(DELIVER_ON, new Date().toInstant().plusSeconds(updatedOffsetTimeInSecond).getEpochSecond()));

        logger.info("Updating unack timeout for queue: '{}' with messageId: '{}'", queueName, messageId);
		return mongoDatabase.getCollection(QUEUE_COLLECTION).updateMany(filter, updatePredicates).wasAcknowledged();
	}

	@Override
	public void flush(String queueName) {
        logger.info("flush for queue: '{}'", queueName);
	    Bson filter = Filters.eq(QUEUE_NAME, queueName);
		boolean ack = mongoDatabase.getCollection(QUEUE_COLLECTION).deleteMany(filter).wasAcknowledged();
		
		if (ack){
	        logger.info("Succussfully deleted messages from queue: '{}'", queueName);
		}else {
	        logger.info("Failed to delete messages from queue: '{}'", queueName);
		}
	}

	@Override
	public Map<String, Long> queuesDetail() {
		logger.error("Implement me!! - queuesDetail");
	    return null;
	}

	@Override
	public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
		logger.error("Implement me!! - queuesDetailVerbose");
		return null;
	}

	@Override
	public boolean resetOffsetTime(String queueName, String messageId) {
        logger.info("resetOffsetTime in '{}' with '{}'", queueName, messageId);
        long offsetTimeInSecond = 0;    
        
	    Bson filter = Filters.eq("_id", getDocumentIDForQueueMessage(queueName, messageId));
	    List<Bson> updatePredicates = new ArrayList<Bson>();
	    updatePredicates.add(Updates.set(OFFSET_TIME_SECONDS, offsetTimeInSecond));
	    updatePredicates.add(Updates.set(DELIVER_ON, new Date().toInstant().plusSeconds(offsetTimeInSecond)));

        logger.info("Updating unack timeout for queue: '{}' with messageId: '{}'", queueName, messageId);
		return mongoDatabase.getCollection(QUEUE_COLLECTION).updateMany(filter, updatePredicates).wasAcknowledged();
	}

	private boolean existsMessage(String queueName, String messageID) {
        logger.info("existsMessage in '{}' with '{}'", queueName, messageID);
	    FindIterable<Document> iterable = mongoDatabase.getCollection(QUEUE_COLLECTION).find(new Document("_id", getDocumentIDForQueueMessage(queueName, messageID)));
	    return iterable.first() != null;
	}

	private void pushMessage(String queueName, String messageId, String payload, Integer priority,
			long offsetTimeInSecond) {
        logger.info("pushMessage to '{}'", queueName);
		MongoQueue mongoQueue = new MongoQueue(queueName, messageId, priority, offsetTimeInSecond, payload);
		
		String json = toJson(mongoQueue);
		Document taskDocument = Document.parse(json);
		
		FindOneAndReplaceOptions upsertOption = new FindOneAndReplaceOptions();
		upsertOption.upsert(true);
	    Bson filter = Filters.eq("_id", mongoQueue.hashCode());
	    
        logger.info("Inserting docs for '{}'", queueName);
		mongoDatabase.getCollection(QUEUE_COLLECTION).findOneAndReplace(filter, taskDocument, upsertOption);
//		mongoDatabase.getCollection(QUEUE_COLLECTION).insertOne(taskDocument);
	}
	private List<Message> peekMessages(String queueName, int count) {
        logger.info("peekMessages from '{}'", queueName);
		if (count < 1)
            return Collections.emptyList();
		

		Date date = new Date();
		date.toInstant().plusMillis(1);
		
//	    TODO: Add created On field
	    Bson filter = Filters.and(Filters.eq(QUEUE_NAME, queueName), Filters.eq(POPPED, false), Filters.lte(DELIVER_ON, date.getTime()));
	    Bson sort = Sorts.orderBy(Sorts.descending(PRIORITY), Sorts.ascending(DELIVER_ON));
	    
	    UpdateOptions updateOptions = new UpdateOptions();
	    
	    //1. Fetch the docs
	    FindIterable<Document> taskDefinitions = mongoDatabase.getCollection(QUEUE_COLLECTION).find(filter).sort(sort).limit(count);
	    
	    //2. Parse return values
	    List<Message> results = new ArrayList<>();
	    List<MongoQueue> mongoQueueList = StreamSupport.stream(taskDefinitions.spliterator(), false).map(doc -> readValue(doc.toJson(jsonWriterSettings), MongoQueue.class)).collect(Collectors.toList());
	    //TODO: Refactor this.
	    for(MongoQueue result : mongoQueueList)
	    {
	        System.out.println(result.getMessageID());
            Message m = new Message();
            m.setId(result.getMessageID());
            m.setPriority(result.getPriority());
            m.setPayload(result.getPayload());
            results.add(m);
	    }
	    
	    return results;
	}

	private List<Message> popMessages(String queueName, int count, int timeout) {
		// TODO: vsheoran, mysql persistence add 1 millisecond. Figure out if this is required for mongodb.
        logger.info("popMessages from '{}'", queueName);
        
        long start = System.currentTimeMillis();
        List<Message> messages = peekMessages(queueName, count);

        while (messages.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
//            logger.info("popMessages -- Inside while. MessageSize: '{}', count: '{}', val<timeout: '{}'<'{}'", messages.size(), count, (System.currentTimeMillis() - start), timeout);
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            messages = peekMessages(queueName, count);
        }

        if (messages.isEmpty()) {
//            logger.info("popMessages -- messages present for the queue: '{}'", queueName);
            return messages;
        }

	    //3. Update the fetched docs and set 'popped' to true. Note here we are using the same filter.
        List<Message> poppedMessages = new ArrayList<>();
        for (Message msg: messages) {
    	    Bson filter = Filters.and(Filters.eq(QUEUE_NAME, queueName), Filters.eq(POPPED, false), Filters.eq(MESSAGE_ID, msg.getId()));
    	    UpdateResult result = mongoDatabase.getCollection(QUEUE_COLLECTION).updateOne(filter, Updates.set(POPPED, true));

            if (result.wasAcknowledged()) {
                poppedMessages.add(msg);
            }
        }
        return poppedMessages;
	}


	private boolean removeMessage(String queueName, String messageId) {
        logger.info("removeMessage for queue: '{}' with id: '{}'", queueName, messageId);
	    Bson filter = Filters.eq("_id", getDocumentIDForQueueMessage(queueName, messageId));
		return mongoDatabase.getCollection(QUEUE_COLLECTION).deleteOne(filter).wasAcknowledged();
	}

    
    //    Returns document id for the Queue Collection
    private int getDocumentIDForQueueMessage(String queueName, String messageID) {
    	return Objects.hash(queueName, messageID);
    }

}
