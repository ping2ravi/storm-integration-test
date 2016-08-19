package com.next.storm.integration;

import org.apache.storm.LocalCluster;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.concurrent.TimeUnit;


public interface TestStormTopology {

	boolean startTopology(LocalCluster localCluster, long timeout, TimeUnit timeUnit) throws Exception;
	
	boolean sendMessageToStreamOfBolt(String streamId, String messageId, Values message, long timeout, TimeUnit timeUnit) throws Exception;
	
	List<Values> getMessageReceivedOnStream(String streamId) throws Exception;
	
	boolean killTopology(LocalCluster localCluster) throws Exception;

	boolean waitUntilNMessagesAreProcessed(int n, int maxSecondsToWait);
	
	
}
