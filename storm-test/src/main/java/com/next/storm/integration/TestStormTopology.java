package com.next.storm.integration;

import java.util.concurrent.TimeUnit;

import backtype.storm.LocalCluster;

public interface TestStormTopology {

	boolean startTopology();
	
	boolean startTopology(LocalCluster localCluster);
	
	boolean sendMessageToStreamOfBolt(String streamId, String boltId, long timeout, TimeUnit timeUnit);
	
	
	
	
}
