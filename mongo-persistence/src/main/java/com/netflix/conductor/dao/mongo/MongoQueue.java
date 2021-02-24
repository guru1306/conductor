package com.netflix.conductor.dao.mongo;

import java.time.Instant;
import java.util.Date;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

// NOTE: We use messageID and queueName to uniquely identify the message.
@JsonInclude(content = Include.NON_NULL)
public class MongoQueue {
	
	@JsonProperty("_id")
	private Integer id;

	@JsonProperty("queue_name")
	private String queueName;

	@JsonProperty("message_id")
	private String messageID;

	@JsonProperty("priority")
	private Integer priority;

	@JsonProperty("offset_time_seconds")
	private Long offsetTimeSeconds;

	@JsonProperty("deliver_on")
	private Date deliverOn;

	@JsonProperty("payload")
	private String payload;

	@JsonProperty("popped")
	private Boolean popped;
	
	public MongoQueue() {
		
	}

	public MongoQueue(String queueName, String messageID, Integer priority, long offsetTimeInSecond, String payload) {
		Date date = new Date();
		date.toInstant().plusSeconds(offsetTimeInSecond);
		
		this.queueName = queueName;
		this.messageID = messageID;
		this.priority = priority;
		this.offsetTimeSeconds = offsetTimeInSecond;
		this.deliverOn = date;
		this.payload = payload;
		this.popped = false;
		this.id = hashCode();
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public String getMessageID() {
		return messageID;
	}

	public void setMessageID(String messageID) {
		this.messageID = messageID;
	}

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public Long getOffsetTimeSeconds() {
		return offsetTimeSeconds;
	}

	public void setOffsetTimeSeconds(Long offsetTimeSeconds) {
		this.offsetTimeSeconds = offsetTimeSeconds;
	}

	public Date getDeliverOn() {
		return deliverOn;
	}

	public void setDeliverOn(Date deliverOn) {
		this.deliverOn = deliverOn;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public Boolean getPopped() {
		return popped;
	}

	public void setPopped(Boolean popped) {
		this.popped = popped;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getQueueName(), getMessageID());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MongoQueue other = (MongoQueue) obj;
		if (messageID == null) {
			if (other.messageID != null)
				return false;
		} else if (!messageID.equals(other.messageID))
			return false;
		if (queueName == null) {
			if (other.queueName != null)
				return false;
		} else if (!queueName.equals(other.queueName))
			return false;
		return true;
	}
	
}
