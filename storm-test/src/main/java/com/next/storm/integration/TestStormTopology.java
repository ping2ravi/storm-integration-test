package com.next.storm.integration;

import java.util.List;
import java.util.concurrent.TimeUnit;

import backtype.storm.LocalCluster;
import backtype.storm.tuple.Values;

public interface TestStormTopology {

	boolean startTopology(LocalCluster localCluster, long timeout, TimeUnit timeUnit) throws Exception;
	
	boolean sendMessageToStreamOfBolt(String streamId, String messageId, Values message, long timeout, TimeUnit timeUnit) throws Exception;
	
	List<Values> getMessageReceivedOnStream(String streamId) throws Exception;
	
	boolean killTopology(LocalCluster localCluster) throws Exception;


	
	
}
