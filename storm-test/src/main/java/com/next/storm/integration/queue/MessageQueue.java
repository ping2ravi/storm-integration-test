package com.next.storm.integration.queue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public class MessageQueue implements Serializable{

	private static final long serialVersionUID = 1L;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private MessageQueue(){
		
	}
	private static MessageQueue messageQueue = new MessageQueue();
	private static Map<String, LinkedBlockingQueue<Message>> map = new HashMap<>();

	public static MessageQueue getInstance(){
		return messageQueue;
	}
	public void clearQueues(){
        map.clear();
    }
	public void createMessageStream(String streamId){
		logger.info("Creating Message Stream [{}]", streamId);
        LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();
        map.put(streamId, queue);
    }
    public void addMessageToQueue(String streamId, Message message){
    	LinkedBlockingQueue<Message> queue = getQueue(streamId);
		logger.info("Adding Message [{}] to Queue [{}]", message, streamId);
        queue.offer(message);
    }

    public Message getMessageFromQueue(String streamId){
    	LinkedBlockingQueue<Message> queue = getQueue(streamId);
    	Message message = queue.poll();
		logger.info("Getting Message From  Queue [{}] , message : [{}]", streamId, message);
        return message;
    }
    public List<Message> getMessageQueue(String streamId){
    	LinkedBlockingQueue<Message> queue = getQueue(streamId);
        return new ArrayList<Message>(queue);
    }
    private LinkedBlockingQueue<Message> getQueue(String streamId){
    	LinkedBlockingQueue<Message> queue = map.get(streamId);
    	if(queue == null){
    		throw new RuntimeException("No Stream Found "+streamId+", Available streams are "+StringUtils.collectionToDelimitedString(map.keySet(), ",","[","]"));
    	}
    	return queue;
    }
}
