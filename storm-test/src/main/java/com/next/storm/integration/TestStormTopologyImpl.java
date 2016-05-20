package com.next.storm.integration;

import java.util.concurrent.TimeUnit;

import backtype.storm.LocalCluster;

public class TestStormTopologyImpl implements TestStormTopology {

	public boolean startTopology() {
		return false;
	}

	public boolean startTopology(LocalCluster localCluster) {
		return false;
	}

	public boolean sendMessageToStreamOfBolt(String streamId, String boltId, long timeout, TimeUnit timeUnit) {
		return false;
	}

}
