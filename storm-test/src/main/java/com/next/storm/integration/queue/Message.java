package com.next.storm.integration.queue;

import org.apache.storm.tuple.Values;

import java.io.Serializable;


public class Message implements Serializable{

	private static final long serialVersionUID = 1L;
	private String messageId;
	private Values message;
	public String getMessageId() {
		return messageId;
	}
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}
	public Values getMessage() {
		return message;
	}
	public void setMessage(Values message) {
		this.message = message;
	}
}
